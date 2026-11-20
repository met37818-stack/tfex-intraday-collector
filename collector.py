#!/usr/bin/env python3
"""
TFEX Market Data Collector & Persistent Session Worker
Fetches SET50 Spot Index, S50 Futures, and SET50 Options from Settrade Open API (v2)
and writes batch snapshots into Turso Cloud SQLite via HTTP Pipeline API (/v2/pipeline).

Supports:
- Persistent Session Worker Loop (--session): Runs continuous 1-minute collection
  during morning (09:45-12:30) and afternoon (13:45-16:55) sessions.
- Single Snapshot (--once): Captures 1 minute snapshot and exits.
- Telegram Checkpoint Inspection (--notify): Sends manual status report to Telegram.
"""

import os
import sys
import re
import time as pytime
import argparse
import logging
import threading
from datetime import datetime, time, date, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
import requests
import settrade_v2.context as st_context
import settrade_v2.logger as st_logger
from settrade_v2 import Investor, config
from settrade_v2.context import RateLimit
from settrade_v2.errors import SettradeError

# ==============================================================================
# Logging Configuration
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("tfex_collector")

# Silence high-frequency per-symbol debug/error noise from Settrade SDK log wrapper
# (e.g. unlisted strikes legitimately throwing Symbol not found)
st_logger.logger.setLevel(logging.CRITICAL + 1)
logging.getLogger("settrade_v2").setLevel(logging.CRITICAL + 1)

# Timezone constant
BANGKOK_TZ = pytz.timezone("Asia/Bangkok")

# Strike grid used to address the SET50 option board. Deliberately wide: the
# worker subscribes to everything and lets unlisted strikes answer "not found",
# so a stale grid only costs a few wasted subscriptions.
STRIKE_STEP = 10
STRIKE_MIN = 750
STRIKE_MAX = 1350
STRIKE_ATM_RANGE = 250
DEFAULT_ATM_BASE = 920.0

# Shared calendar helpers (tfex_common, pinned to a tag in requirements.txt).
from tfex_common.calendar import (  # noqa: E402
    SET_HOLIDAYS,
    get_contract_last_trading_day,
    get_penultimate_business_day,
    resolve_active_series_candidates,
)
# Telegram dispatch is shared too. BANGKOK_TZ stays local: it is a pytz zone used
# with .localize() in 20+ places, which the package's fixed-offset tzinfo lacks.
from tfex_common.telegram import send_alert  # noqa: E402
from tfex_common.turso import execute_pipeline  # noqa: E402
from tfex_common.turso import normalize_url as normalize_turso_pipeline_url  # noqa: E402
from tfex_common.turso import to_arg as to_turso_arg  # noqa: E402


# The SET holiday table now comes from tfex_common.calendar, so this repo can no
# longer disagree with the EOD collector about whether a day is a trading session.

# Session schedule definitions (Asia/Bangkok)
SESSION_CONFIG = {
    "MORNING": {
        "name": "MORNING",
        "wakeup_start": time(9, 0),
        "pre_warm_time": time(9, 44, 30),
        "market_open": time(9, 45),
        "verify_holiday": time(9, 55),
        "market_close": time(12, 30),
        "cutoff_time": time(12, 30, 5),
        "session_end": time(12, 30, 10),
    },
    "AFTERNOON": {
        "name": "AFTERNOON",
        "wakeup_start": time(13, 0),
        "pre_warm_time": time(13, 44, 30),
        "market_open": time(13, 45),
        "verify_holiday": time(13, 55),
        "market_close": time(16, 55),
        "cutoff_time": time(16, 55, 5),
        "session_end": time(16, 55, 10),
    }
}

# ==============================================================================
# Performance & High-Throughput Connection Optimization (60 req/sec Market Data)
# ==============================================================================
# 1. Update settrade_v2 default RateLimit to Settrade Market Data specs (60 req/sec)
_orig_ratelimit_init = RateLimit.__init__
def _market_data_ratelimit_init(self, *args, **kwargs):
    _orig_ratelimit_init(self, *args, **kwargs)
    self.rate_per_second = 60
    self.remaining_per_second = 60
    self.rate_per_minute = 3600
    self.remaining_per_minute = 3600
try:
    RateLimit.__init__ = _market_data_ratelimit_init
except Exception as patch_err:  # pragma: no cover - depends on SDK internals
    logger.warning(f"[SETTRADE] Could not patch RateLimit defaults: {patch_err}")

# 2. Patch settrade_v2 with a persistent requests.Session per thread for
#    HTTP Keep-Alive connection reuse.
#    Session is not thread-safe (its connection pool is shared mutable state),
#    and option quotes are fetched through ThreadPoolExecutor(max_workers=N);
#    a single shared session raced across those workers.
_http_adapter = requests.adapters.HTTPAdapter(
    pool_connections=25,
    pool_maxsize=25,
    max_retries=1
)
_thread_local = threading.local()


def _http_session() -> requests.Session:
    """One requests.Session per thread, sharing the tuned connection pool adapter."""
    session = getattr(_thread_local, "http", None)
    if session is None:
        session = requests.Session()
        session.mount("https://", _http_adapter)
        session.mount("http://", _http_adapter)
        _thread_local.http = session
    return session


def _patched_request(method: str, url: str, **kwargs):
    return _http_session().request(method=method, url=url, **kwargs)


st_context.request = _patched_request


class PacedRateLimiter:
    """
    Precision Token-Bucket style pacer to ensure concurrent threads
    smoothly stay below Settrade's 60 req/s gateway burst limit.
    """
    def __init__(self, max_rps: float = 45.0):
        self.interval = 1.0 / max_rps
        self.lock = threading.Lock()
        self.last_req_time = pytime.perf_counter()

    def acquire(self) -> None:
        with self.lock:
            now = pytime.perf_counter()
            elapsed = now - self.last_req_time
            if elapsed < self.interval:
                pytime.sleep(self.interval - elapsed)
            self.last_req_time = pytime.perf_counter()


# ==============================================================================
# 1. Trading Hours & Session Detection
# ==============================================================================
def detect_session_window(now_bkk: datetime) -> Optional[Dict[str, Any]]:
    """
    Identifies if current Asia/Bangkok time falls into MORNING or AFTERNOON worker window.
    - MORNING: 09:00 - 12:30:10 (Wakeup ~09:15, Pre-warm <=09:44:30, Open 09:45, Cutoff 12:30:05)
    - AFTERNOON: 13:00 - 16:55:10 (Wakeup ~13:15, Pre-warm <=13:44:30, Open 13:45, Cutoff 16:55:05)
    """
    curr_t = now_bkk.time()
    if time(9, 0) <= curr_t <= time(12, 30, 10):
        return SESSION_CONFIG["MORNING"]
    if time(13, 0) <= curr_t <= time(16, 55, 10):
        return SESSION_CONFIG["AFTERNOON"]
    return None


def is_tick_within_session(now_t: time) -> bool:
    """True only inside a continuous session: 09:45:00-12:30:05 or 13:45:00-16:55:05.

    The previous guard only rejected the lunch break and everything after
    16:55:05, so ticks arriving during the 09:00-09:44 pre-open window were
    recorded as snapshots.
    """
    return (time(9, 45) <= now_t <= time(12, 30, 5)) or (time(13, 45) <= now_t <= time(16, 55, 5))


def is_within_trading_hours(dt: datetime) -> Tuple[bool, str]:
    """
    Checks if current Asia/Bangkok time is within official TFEX trading sessions:
    - Monday to Friday only
    - Morning Session:   09:45 - 12:30:05
    - Afternoon Session: 13:45 - 16:55:05
    """
    weekday = dt.weekday()
    if weekday > 4:
        day_name = dt.strftime("%A")
        return False, f"Weekend ({day_name})"

    current_time = dt.time()
    morning_open = time(9, 45)
    morning_close = time(12, 30, 5)
    afternoon_open = time(13, 45)
    afternoon_close = time(16, 55, 5)

    if morning_open <= current_time <= morning_close:
        return True, "Morning Session (09:45 - 12:30)"
    if afternoon_open <= current_time <= afternoon_close:
        return True, "Afternoon Session (13:45 - 16:55)"

    return False, f"Outside sessions (Current time: {dt.strftime('%H:%M:%S')})"


# ==============================================================================
# 2. Dynamic TFEX Contract Resolution (Rollover Proof)
# ==============================================================================


def generate_all_active_strikes(atm_base: Optional[float] = None) -> List[int]:
    """
    Generates all active strike prices covering the entire TFEX trading board.
    Uses 10-point increments, covering 750 to 1350 and ATM ± 250 points.
    """
    min_strike = STRIKE_MIN
    max_strike = STRIKE_MAX
    if atm_base and atm_base > 0:
        atm_strike = round(atm_base / float(STRIKE_STEP)) * STRIKE_STEP
        min_strike = min(min_strike, int(atm_strike - STRIKE_ATM_RANGE))
        max_strike = max(max_strike, int(atm_strike + STRIKE_ATM_RANGE))

    min_strike = int(min_strike // STRIKE_STEP * STRIKE_STEP)
    max_strike = int(max_strike // STRIKE_STEP * STRIKE_STEP)
    return list(range(min_strike, max_strike + STRIKE_STEP, STRIKE_STEP))


def generate_all_stream_symbols(
    front_series: str,
    next_series: str,
    futures_candidates: List[str],
    all_strikes: List[int]
) -> List[str]:
    """
    Generates all symbols across the entire active TFEX options and futures board.
    No strike is skipped: covers all candidate futures, plus all strikes for both
    Front and Next options series (Call and Put).
    """
    symbols: List[str] = []
    # 1. Futures
    for series_tag in futures_candidates:
        symbols.append(f"S50{series_tag}")

    # 2. Options (All strikes for Front and Next series)
    for opt_series in [front_series, next_series]:
        for k in all_strikes:
            symbols.append(f"S50{opt_series}C{k}")
            symbols.append(f"S50{opt_series}P{k}")

    return symbols


# ==============================================================================
# 3. Data Extraction Helpers
# ==============================================================================
def safe_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def extract_spot_data(raw: Dict[str, Any]) -> Dict[str, Optional[float]]:
    last = raw.get("last") or raw.get("last_price") or raw.get("lastPrice")
    vol = raw.get("total_volume") or raw.get("totalVolume") or raw.get("volume")
    val = raw.get("total_value") or raw.get("totalValue") or raw.get("value")
    return {
        "last": safe_float(last),
        "volume": safe_float(vol) or 0.0,
        "value": safe_float(val) or 0.0
    }


def extract_derivatives_data(raw: Dict[str, Any]) -> Dict[str, Optional[float]]:
    bid = raw.get("bid") or raw.get("bid_price1") or raw.get("bidPrice1") or raw.get("best_bid")
    ask = raw.get("ask") or raw.get("ask_price1") or raw.get("askPrice1") or raw.get("offer") or raw.get("best_ask")
    last = raw.get("last") or raw.get("last_price") or raw.get("lastPrice")
    vol = raw.get("total_volume") or raw.get("totalVolume") or raw.get("volume")
    oi = raw.get("open_interest") or raw.get("openInterest") or raw.get("oi") or raw.get("prior_oi")

    if bid is None and "bids" in raw and isinstance(raw["bids"], list) and raw["bids"]:
        bid = raw["bids"][0].get("price")
    if ask is None and "asks" in raw and isinstance(raw["asks"], list) and raw["asks"]:
        ask = raw["asks"][0].get("price")

    return {
        "bid": safe_float(bid),
        "ask": safe_float(ask),
        "last": safe_float(last),
        "volume": safe_float(vol) or 0.0,
        "open_interest": safe_float(oi) if oi is not None else None
    }


# ==============================================================================
# 4. Turso Cloud SQLite HTTP Pipeline API (/v2/pipeline)
# ==============================================================================


# ==============================================================================
# Turso schema (created idempotently on every batch write)
# ==============================================================================
ddl_spot = """
CREATE TABLE IF NOT EXISTS spot_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_bkk TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last REAL,
    volume REAL,
    value REAL
);
"""
ddl_futures = """
CREATE TABLE IF NOT EXISTS futures_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_bkk TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bid REAL,
    ask REAL,
    bid_vol REAL,
    ask_vol REAL,
    last REAL,
    volume REAL,
    open_interest REAL
);
"""
ddl_options = """
CREATE TABLE IF NOT EXISTS options_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_bkk TEXT NOT NULL,
    symbol TEXT NOT NULL,
    series TEXT NOT NULL,
    strike REAL NOT NULL,
    option_type TEXT NOT NULL,
    bid REAL,
    ask REAL,
    bid_vol REAL,
    ask_vol REAL,
    last REAL,
    volume REAL,
    open_interest REAL
);
"""
ddl_telegram_log = """
CREATE TABLE IF NOT EXISTS intraday_telegram_log (
    trade_date TEXT NOT NULL,
    checkpoint TEXT NOT NULL,
    notified_at_bkk TEXT NOT NULL,
    PRIMARY KEY (trade_date, checkpoint)
);
"""

DDL_STATEMENTS: List[str] = [ddl_spot, ddl_futures, ddl_options, ddl_telegram_log]


def build_turso_pipeline_payload(
    spot_rows: List[Dict[str, Any]],
    futures_rows: List[Dict[str, Any]],
    options_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    requests_list: List[Dict[str, Any]] = []

    for ddl in DDL_STATEMENTS:
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": ddl.strip()}
        })

    insert_spot_sql = """
    INSERT INTO spot_snapshots (captured_at_bkk, symbol, last, volume, value)
    VALUES (?, ?, ?, ?, ?);
    """
    for r in spot_rows:
        args = [
            to_turso_arg(r["captured_at_bkk"]),
            to_turso_arg(r["symbol"]),
            to_turso_arg(r["last"]),
            to_turso_arg(r["volume"]),
            to_turso_arg(r["value"]),
        ]
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": insert_spot_sql.strip(), "args": args}
        })

    insert_futures_sql = """
    INSERT INTO futures_snapshots (captured_at_bkk, symbol, bid, ask, bid_vol, ask_vol, last, volume, open_interest)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in futures_rows:
        args = [
            to_turso_arg(r["captured_at_bkk"]),
            to_turso_arg(r["symbol"]),
            to_turso_arg(r.get("bid")),
            to_turso_arg(r.get("ask")),
            to_turso_arg(r.get("bid_vol")),
            to_turso_arg(r.get("ask_vol")),
            to_turso_arg(r.get("last")),
            to_turso_arg(r.get("volume")),
            to_turso_arg(r.get("open_interest")),
        ]
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": insert_futures_sql.strip(), "args": args}
        })

    insert_options_sql = """
    INSERT INTO options_snapshots (
        captured_at_bkk, symbol, series, strike, option_type, bid, ask, bid_vol, ask_vol, last, volume, open_interest
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in options_rows:
        args = [
            to_turso_arg(r["captured_at_bkk"]),
            to_turso_arg(r["symbol"]),
            to_turso_arg(r["series"]),
            to_turso_arg(r["strike"]),
            to_turso_arg(r["option_type"]),
            to_turso_arg(r.get("bid")),
            to_turso_arg(r.get("ask")),
            to_turso_arg(r.get("bid_vol")),
            to_turso_arg(r.get("ask_vol")),
            to_turso_arg(r.get("last")),
            to_turso_arg(r.get("volume")),
            to_turso_arg(r.get("open_interest")),
        ]
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": insert_options_sql.strip(), "args": args}
        })

    requests_list.append({"type": "close"})
    return {"requests": requests_list}


def execute_turso_pipeline(pipeline_url: str, auth_token: str, payload: Dict[str, Any]) -> None:
    """
    Batch write adapter over tfex_common.turso.execute_pipeline().

    The transport (retry on network/429/5xx, token redaction, statement-error
    detection) lives in the package; this keeps the local logging and the
    "returns None / raises" contract the CDC writer expects.
    """
    try:
        execute_pipeline(pipeline_url, auth_token, payload=payload, timeout=30)
    except requests.RequestException as exc:
        logger.error(f"Turso network connection failed: {type(exc).__name__}")
        raise
    except RuntimeError as exc:
        logger.error(f"Turso pipeline failed: {exc}")
        raise
    logger.info("Turso HTTP pipeline batch write committed successfully.")


def ensure_database_schema(pipeline_url: str, auth_token: str) -> None:
    """Ensures bid_vol and ask_vol columns exist on futures_snapshots and options_snapshots."""
    alter_stmts = [
        "ALTER TABLE futures_snapshots ADD COLUMN bid_vol REAL;",
        "ALTER TABLE futures_snapshots ADD COLUMN ask_vol REAL;",
        "ALTER TABLE options_snapshots ADD COLUMN bid_vol REAL;",
        "ALTER TABLE options_snapshots ADD COLUMN ask_vol REAL;"
    ]
    for sql in alter_stmts:
        req = {"requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}
        try:
            query_turso_pipeline(pipeline_url, auth_token, req["requests"])
        except Exception:
            pass  # Column already exists or error ignored safely


def is_tick_changed(prev: Optional[Dict[str, Any]], curr: Dict[str, Any]) -> bool:
    """
    Evaluates whether a market tick represents a meaningful change across 6 dimensions:
    - Best Bid price
    - Best Ask price
    - Best Bid volume (depth)
    - Best Ask volume (depth)
    - Last traded price
    - Cumulative traded volume
    """
    if prev is None:
        return True
    return (
        prev.get("bid") != curr.get("bid") or
        prev.get("ask") != curr.get("ask") or
        prev.get("bid_vol") != curr.get("bid_vol") or
        prev.get("ask_vol") != curr.get("ask_vol") or
        prev.get("last") != curr.get("last") or
        prev.get("volume") != curr.get("volume")
    )


def save_cdc_batch_to_turso(
    pipeline_url: str,
    auth_token: str,
    futures_rows: List[Dict[str, Any]],
    options_rows: List[Dict[str, Any]],
    spot_rows: Optional[List[Dict[str, Any]]] = None
) -> None:
    if not futures_rows and not options_rows and not spot_rows:
        return
    payload = build_turso_pipeline_payload(
        spot_rows or [],
        futures_rows,
        options_rows
    )
    execute_turso_pipeline(pipeline_url, auth_token, payload)


# ==============================================================================
# 5. Turso Query Engine & Telegram Status Checkpoint Alerts
# ==============================================================================
def parse_turso_value(val_dict: Dict[str, Any]) -> Any:
    if not val_dict or val_dict.get("type") == "null":
        return None
    val_type = val_dict.get("type")
    val = val_dict.get("value")
    if val_type == "integer":
        try:
            return int(val)
        except (ValueError, TypeError):
            return 0
    if val_type == "float":
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0
    return str(val) if val is not None else None


def parse_turso_result_rows(res: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols = [c["name"] for c in res.get("cols", [])]
    out: List[Dict[str, Any]] = []
    for row in res.get("rows", []):
        row_dict: Dict[str, Any] = {}
        for col_name, val_cell in zip(cols, row):
            row_dict[col_name] = parse_turso_value(val_cell)
        out.append(row_dict)
    return out


def query_turso_pipeline(pipeline_url: str, auth_token: str, requests_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Read adapter over tfex_common.turso.execute_pipeline() (returns raw results)."""
    return execute_pipeline(pipeline_url, auth_token, requests_list=requests_list, timeout=20)


def ensure_telegram_log_table(pipeline_url: str, auth_token: str) -> None:
    ddl_tg = """
    CREATE TABLE IF NOT EXISTS intraday_telegram_log (
        trade_date TEXT NOT NULL,
        checkpoint TEXT NOT NULL,
        notified_at_bkk TEXT NOT NULL,
        PRIMARY KEY (trade_date, checkpoint)
    );
    """
    ddl_err = """
    CREATE TABLE IF NOT EXISTS intraday_tick_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date TEXT NOT NULL,
        captured_at_bkk TEXT NOT NULL,
        session_name TEXT,
        error_type TEXT,
        error_message TEXT
    );
    """
    reqs = [
        {"type": "execute", "stmt": {"sql": ddl_tg.strip()}},
        {"type": "execute", "stmt": {"sql": ddl_err.strip()}},
        {"type": "close"}
    ]
    try:
        query_turso_pipeline(pipeline_url, auth_token, reqs)
    except Exception as e:
        logger.warning(f"Could not verify intraday tables: {e}")


def record_intraday_tick_error(
    pipeline_url: str,
    auth_token: str,
    trade_date: str,
    captured_at: str,
    session_name: str,
    error_type: str,
    error_msg: str
) -> None:
    sql = """
    INSERT INTO intraday_tick_errors (trade_date, captured_at_bkk, session_name, error_type, error_message)
    VALUES (?, ?, ?, ?, ?);
    """
    args = [
        to_turso_arg(trade_date),
        to_turso_arg(captured_at),
        to_turso_arg(session_name),
        to_turso_arg(error_type),
        to_turso_arg(error_msg[:300])
    ]
    reqs = [
        {"type": "execute", "stmt": {"sql": sql.strip(), "args": args}},
        {"type": "close"}
    ]
    try:
        query_turso_pipeline(pipeline_url, auth_token, reqs)
    except Exception as e:
        logger.warning(f"Failed to record tick error into intraday_tick_errors: {e}")


def is_checkpoint_already_notified(pipeline_url: str, auth_token: str, trade_date: str, checkpoint: str) -> bool:
    sql = "SELECT COUNT(*) as cnt FROM intraday_telegram_log WHERE trade_date = ? AND checkpoint = ?;"
    args = [to_turso_arg(trade_date), to_turso_arg(checkpoint)]
    reqs = [
        {"type": "execute", "stmt": {"sql": sql, "args": args}},
        {"type": "close"}
    ]
    try:
        results = query_turso_pipeline(pipeline_url, auth_token, reqs)
        if results and results[0].get("type") == "ok":
            res = results[0]["response"]["result"]
            rows = parse_turso_result_rows(res)
            if rows and (rows[0].get("cnt") or 0) > 0:
                return True
    except Exception as e:
        logger.warning(f"Error checking intraday_telegram_log for {checkpoint}: {e}")
    return False


def record_telegram_notification(
    pipeline_url: str,
    auth_token: str,
    trade_date: str,
    checkpoint: str,
    notified_at: str
) -> None:
    sql = """
    INSERT OR REPLACE INTO intraday_telegram_log (trade_date, checkpoint, notified_at_bkk)
    VALUES (?, ?, ?);
    """
    args = [
        to_turso_arg(trade_date),
        to_turso_arg(checkpoint),
        to_turso_arg(notified_at),
    ]
    reqs = [
        {"type": "execute", "stmt": {"sql": sql.strip(), "args": args}},
        {"type": "close"}
    ]
    try:
        query_turso_pipeline(pipeline_url, auth_token, reqs)
    except Exception as e:
        logger.error(f"Failed to record checkpoint {checkpoint} into intraday_telegram_log: {e}")


def has_intraday_data_for_date(pipeline_url: str, auth_token: str, trade_date: str) -> bool:
    sql = "SELECT COUNT(*) as cnt FROM spot_snapshots WHERE captured_at_bkk LIKE ?;"
    reqs = [
        {"type": "execute", "stmt": {"sql": sql, "args": [to_turso_arg(f"{trade_date}%")]}},
        {"type": "close"}
    ]
    try:
        results = query_turso_pipeline(pipeline_url, auth_token, reqs)
        if results and results[0].get("type") == "ok":
            res = results[0]["response"]["result"]
            rows = parse_turso_result_rows(res)
            if rows and (rows[0].get("cnt") or 0) > 0:
                return True
    except Exception as e:
        logger.warning(f"Error checking intraday data existence for {trade_date}: {e}")
    return False


def is_worker_already_active(pipeline_url: str, auth_token: str, now_bkk: datetime, max_age_seconds: int = 180) -> bool:
    """
    Checks if another worker has captured ticks in futures_snapshots today within the last max_age_seconds (default 3 min).
    Prevents duplicate concurrent workers (e.g. backup cron trigger while primary is running).
    """
    today_str = now_bkk.strftime("%Y-%m-%d")
    sql = """
    SELECT captured_at_bkk 
    FROM futures_snapshots 
    WHERE captured_at_bkk LIKE ? 
    ORDER BY captured_at_bkk DESC 
    LIMIT 1;
    """
    reqs = [
        {"type": "execute", "stmt": {"sql": sql, "args": [to_turso_arg(f"{today_str}%")]}},
        {"type": "close"}
    ]
    try:
        results = query_turso_pipeline(pipeline_url, auth_token, reqs)
        if results and results[0].get("type") == "ok":
            res = results[0]["response"]["result"]
            rows = parse_turso_result_rows(res)
            if rows and rows[0].get("captured_at_bkk"):
                latest_ts_str = str(rows[0]["captured_at_bkk"])
                latest_dt = BANGKOK_TZ.localize(datetime.strptime(latest_ts_str, "%Y-%m-%d %H:%M:%S"))
                diff_sec = (now_bkk - latest_dt).total_seconds()
                if 0 <= diff_sec < max_age_seconds:
                    logger.info(
                        f"[CONCURRENCY GUARD] Found active snapshot captured at {latest_ts_str} "
                        f"({diff_sec:.0f}s ago < {max_age_seconds}s limit)."
                    )
                    return True
    except Exception as e:
        logger.warning(f"[CONCURRENCY GUARD] Warning checking active worker in Turso: {e}")
    return False


def get_latest_turso_trade_date(pipeline_url: str, auth_token: str) -> Optional[str]:
    sql = "SELECT MAX(SUBSTR(captured_at_bkk, 1, 10)) as latest_dt FROM spot_snapshots;"
    reqs = [
        {"type": "execute", "stmt": {"sql": sql}},
        {"type": "close"}
    ]
    try:
        results = query_turso_pipeline(pipeline_url, auth_token, reqs)
        if results and results[0].get("type") == "ok":
            res = results[0]["response"]["result"]
            rows = parse_turso_result_rows(res)
            if rows and rows[0].get("latest_dt"):
                return str(rows[0]["latest_dt"])
    except Exception as e:
        logger.warning(f"Error retrieving latest trade date: {e}")
    return None


def query_turso_intraday_stats(pipeline_url: str, auth_token: str, trade_date: str) -> Dict[str, Any]:
    try:
        dt = datetime.strptime(trade_date, "%Y-%m-%d").replace(tzinfo=BANGKOK_TZ)
    except Exception:
        dt = datetime.now(BANGKOK_TZ)
    cand_fut, front_opt, _ = resolve_active_series_candidates(dt)
    front_fut_symbol = f"S50{front_opt}"

    like_prefix = to_turso_arg(f"{trade_date}%")
    morning_from = to_turso_arg(f"{trade_date} 09:45:00")
    morning_to = to_turso_arg(f"{trade_date} 12:30:05")
    afternoon_from = to_turso_arg(f"{trade_date} 13:45:00")
    afternoon_to = to_turso_arg(f"{trade_date} 16:55:05")
    date_arg = to_turso_arg(trade_date)
    front_fut_arg = to_turso_arg(front_fut_symbol)

    # Every value is bound (?) rather than interpolated: these dates come from the
    # clock or from a value read back out of the database, and a single quote in
    # either would have rewritten the query.
    sql_queries: List[Tuple[str, List[Dict[str, Any]]]] = [
        # 0. spot stats
        ("SELECT COUNT(*) as spot_count, MIN(captured_at_bkk) as min_ts, MAX(captured_at_bkk) as max_ts "
         "FROM spot_snapshots WHERE captured_at_bkk LIKE ?;", [like_prefix]),
        # 1. latest spot
        ("SELECT last, volume, value, captured_at_bkk FROM spot_snapshots "
         "WHERE captured_at_bkk LIKE ? ORDER BY captured_at_bkk DESC LIMIT 1;", [like_prefix]),
        # 2. futures stats
        ("SELECT COUNT(*) as fut_count, COUNT(DISTINCT symbol) as fut_syms FROM futures_snapshots "
         "WHERE captured_at_bkk LIKE ?;", [like_prefix]),
        # 3. latest front futures (front quarter series first, then any valid last price)
        ("""SELECT symbol, last, bid, ask, volume, open_interest, captured_at_bkk
FROM futures_snapshots
WHERE captured_at_bkk LIKE ?
ORDER BY (CASE WHEN symbol = ? AND last IS NOT NULL AND last > 0 THEN 3
               WHEN symbol = ? THEN 2
               WHEN last IS NOT NULL AND last > 0 THEN 1 ELSE 0 END) DESC,
         captured_at_bkk DESC
LIMIT 1;""", [like_prefix, front_fut_arg, front_fut_arg]),
        # 4. options stats
        ("SELECT COUNT(*) as opt_count, COUNT(DISTINCT symbol) as opt_syms FROM options_snapshots "
         "WHERE captured_at_bkk LIKE ?;", [like_prefix]),
        # 5. morning bars
        ("SELECT COUNT(*) as morning_bars, MIN(captured_at_bkk) as min_morning, MAX(captured_at_bkk) as max_morning "
         "FROM spot_snapshots WHERE captured_at_bkk >= ? AND captured_at_bkk <= ?;",
         [morning_from, morning_to]),
        # 6. afternoon bars
        ("SELECT COUNT(*) as afternoon_bars, MIN(captured_at_bkk) as min_afternoon, MAX(captured_at_bkk) as max_afternoon "
         "FROM spot_snapshots WHERE captured_at_bkk >= ? AND captured_at_bkk <= ?;",
         [afternoon_from, afternoon_to]),
        # 7. pragma page_count
        ("PRAGMA page_count;", []),
        # 8. pragma page_size
        ("PRAGMA page_size;", []),
        # 9. tick errors count
        ("SELECT COUNT(*) as err_count FROM intraday_tick_errors WHERE trade_date = ?;", [date_arg]),
    ]

    reqs = [
        {"type": "execute", "stmt": ({"sql": sql, "args": args} if args else {"sql": sql})}
        for sql, args in sql_queries
    ]
    reqs.append({"type": "close"})

    results = query_turso_pipeline(pipeline_url, auth_token, reqs)
    stats: Dict[str, Any] = {}

    def get_first_row(idx: int) -> Dict[str, Any]:
        if idx < len(results) and results[idx].get("type") == "ok":
            res = results[idx]["response"]["result"]
            rows = parse_turso_result_rows(res)
            if rows:
                return rows[0]
        return {}

    # 0. Spot count and bounds
    r0 = get_first_row(0)
    stats["spot_count"] = r0.get("spot_count") or 0
    stats["min_ts"] = r0.get("min_ts") or "-"
    stats["max_ts"] = r0.get("max_ts") or "-"

    # 1. Latest spot
    r1 = get_first_row(1)
    stats["spot_last"] = r1.get("last")
    stats["spot_vol"] = r1.get("volume") or 0.0
    stats["spot_val"] = r1.get("value") or 0.0

    # 2. Futures stats
    r2 = get_first_row(2)
    stats["fut_count"] = r2.get("fut_count") or 0
    stats["fut_syms"] = r2.get("fut_syms") or 0

    # 3. Latest front futures
    r3 = get_first_row(3)
    stats["fut_symbol"] = r3.get("symbol") or front_fut_symbol
    stats["fut_last"] = r3.get("last")
    stats["fut_bid"] = r3.get("bid")
    stats["fut_ask"] = r3.get("ask")
    stats["fut_oi"] = r3.get("open_interest") or 0.0

    # If last traded price is None in r3, fall back to mid-price if bid and ask exist
    if stats["fut_last"] is None and stats.get("fut_bid") is not None and stats.get("fut_ask") is not None:
        stats["fut_last"] = round((stats["fut_bid"] + stats["fut_ask"]) / 2.0, 2)

    # 4. Options stats
    r4 = get_first_row(4)
    stats["opt_count"] = r4.get("opt_count") or 0
    stats["opt_syms"] = r4.get("opt_syms") or 0

    # 5. Morning bars
    r5 = get_first_row(5)
    stats["morning_bars"] = r5.get("morning_bars") or 0
    stats["min_morning"] = r5.get("min_morning") or "-"
    stats["max_morning"] = r5.get("max_morning") or "-"

    # 6. Afternoon bars
    r6 = get_first_row(6)
    stats["afternoon_bars"] = r6.get("afternoon_bars") or 0
    stats["min_afternoon"] = r6.get("min_afternoon") or "-"
    stats["max_afternoon"] = r6.get("max_afternoon") or "-"

    # 7 & 8. Database size & Turso 5 GB Quota
    r7 = get_first_row(7)
    r8 = get_first_row(8)
    page_count = r7.get("page_count") or 0
    page_size = r8.get("page_size") or 4096
    db_bytes = page_count * page_size
    db_mb = db_bytes / (1024.0 * 1024.0)
    quota_mb = 5120.0  # 5 GB free tier quota
    quota_pct = (db_mb / quota_mb) * 100.0

    stats["db_pages"] = page_count
    stats["db_bytes"] = db_bytes
    stats["db_mb"] = db_mb
    stats["quota_pct"] = quota_pct

    if db_bytes >= 1024 * 1024:
        db_raw_str = f"{db_mb:.2f} MB"
    else:
        db_raw_str = f"{db_bytes / 1024.0:.1f} KB"

    quota_warn = " 🚨 <b>ใกล้เต็มโควต้า 5 GB กรุณาเคลียร์ข้อมูลเก่า</b>" if quota_pct >= 80.0 else ""
    stats["db_size_str"] = f"{db_raw_str} ({quota_pct:.2f}% ของโควต้า 5 GB){quota_warn}"

    # 9. Error count
    r9 = get_first_row(9)
    stats["error_count"] = r9.get("err_count") or 0

    return stats


def get_current_checkpoint(t: time) -> Optional[str]:
    """
    4-Checkpoint alert schedules:
    1. MORNING_OPEN: 10:00 (10:00 - 12:29:59) - Active across morning session once market opens
    2. MIDDAY_CLOSE: 12:30 (12:30 - 13:44:59) - Midday break report
    3. AFTERNOON_OPEN: 14:00 (14:00 - 16:54:59) - Active across afternoon session once market opens (15 min after 13:45 open)
    4. EVENING_CLOSE: 16:55 (16:55 - 18:30) - Full-day close summary
    """
    if time(10, 0) <= t < time(12, 30):
        return "MORNING_OPEN"
    if time(12, 30) <= t < time(13, 45):
        return "MIDDAY_CLOSE"
    if time(14, 0) <= t < time(16, 55):
        return "AFTERNOON_OPEN"
    if time(16, 55) <= t <= time(18, 30):
        return "EVENING_CLOSE"
    return None


def build_checkpoint_message(
    checkpoint: str,
    trade_date: str,
    now_bkk: datetime,
    stats: Dict[str, Any],
    is_manual: bool = False,
    cdc_stats: Optional[Dict[str, int]] = None
) -> str:
    date_str = now_bkk.strftime("%Y%m%d")
    time_str = now_bkk.strftime("%H:%M")

    topic_map = {
        "MORNING_OPEN": "Morning Open Status",
        "MIDDAY_CLOSE": "Midday Break Report",
        "AFTERNOON_OPEN": "Afternoon Open Status",
        "EVENING_CLOSE": "Full-Day Close Summary",
        "MANUAL_REPORT": "Manual Status Inspection"
    }
    topic_desc = topic_map.get(checkpoint, checkpoint)

    header = (
        f"Date: {date_str} | Time : {time_str} | Topic : TFEX Collector - {topic_desc}\n"
        f"Model : Settrade v2 Collector | API No: Turso SQLite Pipeline"
    )

    spot_count = stats.get("spot_count", 0)
    spot_last = stats.get("spot_last")
    spot_vol = stats.get("spot_vol", 0.0)

    fut_count = stats.get("fut_count", 0)
    fut_sym = stats.get("fut_symbol", "S50")
    fut_last = stats.get("fut_last")
    fut_oi = stats.get("fut_oi", 0.0)

    opt_count = stats.get("opt_count", 0)
    opt_syms = stats.get("opt_syms", 0)

    morning_bars = stats.get("morning_bars", 0)
    afternoon_bars = stats.get("afternoon_bars", 0)
    min_ts = stats.get("min_ts", "-")
    max_ts = stats.get("max_ts", "-")
    db_size = stats.get("db_size_str", "-")
    db_pages = stats.get("db_pages", 0)
    error_count = stats.get("error_count", 0)
    err_str = "0 ครั้ง" if error_count == 0 else f"⚠️ {error_count} ครั้ง"

    spot_str = f"{spot_last:.2f}" if spot_last is not None else "N/A"
    fut_str = f"{fut_last:.2f}" if fut_last is not None else "N/A"

    mode_tag = " <i>(Manual Test Trigger)</i>" if is_manual else ""

    if checkpoint == "MORNING_OPEN":
        body = (
            f"🔔 <b>TFEX INTRADAY: MORNING OPEN STATUS</b>{mode_tag}\n\n"
            f"• <b>Status:</b> 🟢 Morning Session Active (09:45 - 12:30)\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Latest Timestamp:</b> <code>{max_ts}</code>\n"
            f"• <b>Bars Captured (Today):</b> <b>{spot_count}</b> bars\n"
            f"• <b>SET50 Spot:</b> <code>{spot_str}</code> (Vol: {spot_vol:,.0f})\n"
            f"• <b>Front Futures ({fut_sym}):</b> <code>{fut_str}</code> (OI: {fut_oi:,.0f})\n"
            f"• <b>Active Options:</b> <b>{opt_syms}</b> contracts ({opt_count} snaps)\n"
            f"• <b>API/DB Errors:</b> {err_str}\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}"
        )
    elif checkpoint == "MIDDAY_CLOSE":
        m_pct = (morning_bars / 165.0) * 100.0 if morning_bars else 0.0
        health = "🟢 Complete" if m_pct >= 99.0 else f"🟡 Partial ({morning_bars}/165 bars)"
        body = (
            f"☕ <b>TFEX INTRADAY: MIDDAY BREAK REPORT</b>{mode_tag}\n\n"
            f"• <b>Status:</b> ⏸️ Morning Session Closed\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Morning Span:</b> <code>{min_ts}</code> – <code>{max_ts}</code>\n"
            f"• <b>Morning Bars:</b> <b>{morning_bars}</b> / 165 ({m_pct:.1f}%)\n"
            f"• <b>SET50 Spot:</b> <code>{spot_str}</code> | <b>{fut_sym}:</b> <code>{fut_str}</code>\n"
            f"• <b>Snapshots:</b> Spot: {spot_count} | Fut: {fut_count} | Opt: {opt_count}\n"
            f"• <b>API/DB Errors:</b> {err_str}\n"
            f"• <b>Health Check:</b> {health}\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}"
        )
    elif checkpoint == "AFTERNOON_OPEN":
        warn = "\n⚠️ <b>Alert:</b> No bars captured yet for afternoon session!" if afternoon_bars == 0 else ""
        body = (
            f"📈 <b>TFEX INTRADAY: AFTERNOON OPEN STATUS</b>{mode_tag}\n\n"
            f"• <b>Status:</b> 🟢 Afternoon Session Active (13:45 - 16:55)\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Afternoon Bars (since 13:45):</b> <b>{afternoon_bars}</b> bars\n"
            f"• <b>Latest Timestamp:</b> <code>{max_ts}</code>\n"
            f"• <b>SET50 Spot:</b> <code>{spot_str}</code> | <b>{fut_sym}:</b> <code>{fut_str}</code>\n"
            f"• <b>Active Options:</b> <b>{opt_syms}</b> contracts\n"
            f"• <b>API/DB Errors:</b> {err_str}\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}{warn}"
        )
    else:  # EVENING_CLOSE or MANUAL_REPORT
        total_expected = 355.0
        c_pct = (spot_count / total_expected) * 100.0 if spot_count else 0.0
        if c_pct >= 99.0:
            verdict = "🟢 Complete (≥99%)"
        elif c_pct >= 90.0:
            verdict = f"🟡 Partial ({spot_count}/355 bars)"
        else:
            verdict = f"🚨 Incomplete ({spot_count}/355 bars)"

        title = "🏁 <b>TFEX INTRADAY: FULL-DAY CLOSE SUMMARY</b>" if checkpoint == "EVENING_CLOSE" else "📊 <b>TFEX INTRADAY: CURRENT STATUS REPORT</b>"
        body = (
            f"{title}{mode_tag}\n\n"
            f"• <b>Status:</b> 🔒 Market Closed\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Trading Span:</b> <code>{min_ts}</code> – <code>{max_ts}</code>\n"
            f"• <b>Total Daily Bars:</b> <b>{spot_count}</b> / 355 ({c_pct:.1f}%)\n"
            f"  - Morning (09:45-12:30): {morning_bars} bars\n"
            f"  - Afternoon (13:45-16:55): {afternoon_bars} bars\n"
            f"• <b>Settlement Ref:</b> SET50 <code>{spot_str}</code> | {fut_sym} <code>{fut_str}</code>\n"
            f"• <b>Snapshots Saved:</b> Spot: {spot_count} | Fut: {fut_count} | Opt: {opt_count}\n"
            f"• <b>API/DB Errors:</b> {err_str}\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}\n"
            f"• <b>Integrity Verdict:</b> {verdict}"
        )

    cdc_sec = ""
    if cdc_stats:
        rcv = cdc_stats.get("received", 0)
        chg = cdc_stats.get("changed", 0)
        ded = cdc_stats.get("deduped", 0)
        fls = cdc_stats.get("flushes", 0)
        rows_w = cdc_stats.get("rows_written", 0)
        ded_pct = (ded / rcv * 100.0) if rcv > 0 else 0.0
        cdc_sec = (
            f"\n\n⚡ <b>CDC Stream Stats:</b>\n"
            f"• <b>Ticks:</b> <code>{rcv:,}</code> received | <code>{chg:,}</code> changed\n"
            f"• <b>Noise Filter:</b> <code>{ded:,}</code> filtered ({ded_pct:.1f}% reduction)\n"
            f"• <b>Turso Flushes:</b> <code>{fls:,}</code> batches ({rows_w:,} rows written)"
        )

    return f"{header}\n\n{body}{cdc_sec}"


def send_telegram_message(bot_token: str, chat_id: str, message: str) -> bool:
    """
    Escapes the HTML then dispatches through tfex_common.telegram.send_alert().

    The escaping and the token-safe error handling live in the package (shared with
    the other three repos); this adapter keeps the local call sites and the
    escape=True semantics this collector has always used.
    """
    return send_alert(bot_token, chat_id, message, escape=True, timeout=15)


_last_critical_alert_time: float = 0.0
_critical_alert_lock = threading.Lock()

def send_critical_error_alert(
    telegram_bot_token: str,
    telegram_chat_id: str,
    error_title: str,
    error_detail: str,
    cooldown_seconds: int = 900
) -> bool:
    global _last_critical_alert_time
    if not telegram_bot_token or not telegram_chat_id:
        return False
    with _critical_alert_lock:
        now_time = pytime.time()
        if now_time - _last_critical_alert_time < cooldown_seconds:
            logger.info(f"[CRITICAL ALERT] Suppressed by rate limit cooldown ({now_time - _last_critical_alert_time:.0f}s < {cooldown_seconds}s).")
            return False
        _last_critical_alert_time = now_time

    now_bkk_str = datetime.now(BANGKOK_TZ).strftime("%Y-%m-%d %H:%M:%S")
    msg = (
        f"🚨 <b>TFEX INTRADAY COLLECTOR: CRITICAL FAILURE ALERT</b>\n\n"
        f"• <b>Error:</b> ❌ {error_title}\n"
        f"• <b>Details:</b> <code>{error_detail}</code>\n"
        f"• <b>Timestamp:</b> <code>{now_bkk_str}</code>\n"
        f"• <b>Action:</b> กรุณาตรวจสอบสถานะ Turso Database และ Settrade Open API ด่วน"
    )
    return send_telegram_message(telegram_bot_token, telegram_chat_id, msg)


def check_and_send_checkpoints(
    now_bkk: datetime,
    pipeline_url: str,
    turso_token: str,
    bot_token: str,
    chat_id: str,
    force_notify: bool = False,
    session_errors: int = 0,
    cdc_stats: Optional[Dict[str, int]] = None
) -> bool:
    if not bot_token or not chat_id:
        logger.warning("[TELEGRAM] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID. Checkpoint alerts skipped.")
        return False

    ensure_telegram_log_table(pipeline_url, turso_token)

    today_str = now_bkk.strftime("%Y-%m-%d")
    current_time = now_bkk.time()

    if force_notify:
        checkpoint = get_current_checkpoint(current_time) or "MANUAL_REPORT"
        target_date = today_str
        has_today = has_intraday_data_for_date(pipeline_url, turso_token, today_str)
        if not has_today:
            latest_dt = get_latest_turso_trade_date(pipeline_url, turso_token)
            if latest_dt:
                target_date = latest_dt
                logger.info(f"[NOTIFY] No intraday data for today ({today_str}). Falling back to latest trade date in Turso: {target_date}")
            else:
                logger.warning("[NOTIFY] Turso database contains no intraday data at all.")
                return False

        stats = query_turso_intraday_stats(pipeline_url, turso_token, target_date)
        if session_errors > 0:
            stats["error_count"] = max(stats.get("error_count", 0), session_errors)
        msg = build_checkpoint_message(checkpoint, target_date, now_bkk, stats, is_manual=True, cdc_stats=cdc_stats)
        logger.info(f"[NOTIFY] Sending Telegram report for {target_date} (Checkpoint: {checkpoint})...")
        return send_telegram_message(bot_token, chat_id, msg)

    # Automated schedule rules:
    # 1. Mon - Fri only
    if now_bkk.weekday() > 4:
        return False

    # 2. Determine checkpoint
    checkpoint = get_current_checkpoint(current_time)
    if not checkpoint:
        return False

    # 3. Check de-duplication log
    if is_checkpoint_already_notified(pipeline_url, turso_token, today_str, checkpoint):
        logger.debug(f"[TELEGRAM] Checkpoint {checkpoint} for {today_str} already sent today. Skipping.")
        return False

    # 4. Market-Open Data Guard
    if not has_intraday_data_for_date(pipeline_url, turso_token, today_str):
        logger.info(f"[TELEGRAM] Market-Open Data Guard: No intraday data collected for {today_str}. Skipping {checkpoint} alert.")
        return False

    # 5. Query stats & send
    stats = query_turso_intraday_stats(pipeline_url, turso_token, today_str)
    if session_errors > 0:
        stats["error_count"] = max(stats.get("error_count", 0), session_errors)
    msg = build_checkpoint_message(checkpoint, today_str, now_bkk, stats, is_manual=False, cdc_stats=cdc_stats)

    logger.info(f"[TELEGRAM] Triggering {checkpoint} alert for {today_str}...")
    success = send_telegram_message(bot_token, chat_id, msg)
    if success:
        now_ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")
        record_telegram_notification(pipeline_url, turso_token, today_str, checkpoint, now_ts_str)
        logger.info(f"[TELEGRAM] Recorded checkpoint {checkpoint} for {today_str} into intraday_telegram_log.")
    return success


# ==============================================================================
# 6. Settrade Client Manager & Re-authentication
# ==============================================================================
class SettradeClientManager:
    """
    Manages Settrade Open API connection state, proactive re-authentication,
    and automatic recovery upon token expiration during long-running sessions.
    Also manages a RealtimeDataConnection Connection Pool (MQTT over WebSocket)
    to stream live Bid/Offer depth for 100% of all TFEX strikes without hitting topic limits.
    """
    def __init__(self, app_id: str, app_secret: str, broker_id: str, app_code: str, is_sandbox: bool):
        self.app_id = app_id
        self.app_secret = app_secret
        self.broker_id = broker_id
        self.app_code = app_code
        self.is_sandbox = is_sandbox
        self.investor: Optional[Investor] = None
        self.market_data: Any = None
        self.realtime_connections: List[Any] = []
        self.bid_offer_cache: Dict[str, Dict[str, Any]] = {}
        self.subscribed_topics: Set[str] = set()
        self._cache_lock = threading.Lock()
        self.last_auth_time: float = 0.0
        # Symbol whose ticks are echoed to the log (the front future). Set by the
        # session worker once the active series is resolved, instead of hardcoding
        # a season code that goes stale at every rollover.
        self.debug_log_symbol: Optional[str] = None

        # Change Data Capture (CDC) State & Coalescing Buffer
        self.latest_market_state: Dict[str, Dict[str, Any]] = {}
        self.last_persisted_state: Dict[str, Dict[str, Any]] = {}
        self.coalesced_buffer: Dict[str, Dict[str, Any]] = {}
        self.last_tick_time: float = pytime.time()
        self.cdc_stats: Dict[str, int] = {
            "received": 0,
            "changed": 0,
            "deduped": 0,
            "flushes": 0,
            "rows_written": 0
        }

    def clear_session_cache(self) -> None:
        """
        Clears all in-memory CDC caches between sessions to prevent stale ticks
        from carrying over into new session baseline snapshots.
        """
        with self._cache_lock:
            self.latest_market_state.clear()
            self.last_persisted_state.clear()
            self.coalesced_buffer.clear()
            self.bid_offer_cache.clear()
            logger.info("[CDC] Session caches cleared (latest_market_state, last_persisted_state, coalesced_buffer).")

    def stop_all_realtime(self) -> None:
        """Safely stops and disconnects all MQTT WebSocket connections in the pool."""
        with self._cache_lock:
            for conn in self.realtime_connections:
                try:
                    # The SDK only exposes the private _stop(); prefer a public
                    # name if a future version adds one.
                    stopper = getattr(conn, "stop", None) or getattr(conn, "_stop", None)
                    if callable(stopper):
                        stopper()
                except Exception:
                    pass
            self.realtime_connections.clear()
            self.subscribed_topics.clear()

    def connect(self) -> None:
        broker = self.broker_id
        if self.is_sandbox:
            config["environment"] = "uat"
            if not broker or broker.upper() == "SANDBOX":
                broker = Investor.SANDBOX

        logger.info(f"[SETTRADE] Authenticating Open API (broker: {broker}, sandbox: {self.is_sandbox})...")
        try:
            self.investor = Investor(
                app_id=self.app_id,
                app_secret=self.app_secret,
                broker_id=broker,
                app_code=self.app_code,
                is_auto_queue=False
            )
            self.market_data = self.investor.MarketData()
            self.last_auth_time = pytime.time()
            logger.info("[SETTRADE] Open API authentication successful.")

            with self._cache_lock:
                prev_subs = list(self.subscribed_topics)
            self.stop_all_realtime()

            if prev_subs:
                self.subscribe_symbols(prev_subs)

        except Exception as e:
            logger.error(f"[SETTRADE] Authentication failed: {e}")
            raise RuntimeError(f"Settrade authentication failure: {e}") from e

    def on_bid_offer(self, msg: Any) -> None:
        if not isinstance(msg, dict) or not msg.get("is_success"):
            return
        data = msg.get("data")
        if not isinstance(data, dict):
            return
        sym = data.get("symbol")
        if not sym:
            return

        bid1 = safe_float(data.get("bid_price1"))
        ask1 = safe_float(data.get("ask_price1"))
        bid_vol1 = safe_float(data.get("bid_volume1")) or 0.0
        ask_vol1 = safe_float(data.get("ask_volume1")) or 0.0
        last_p = safe_float(data.get("last"))
        vol = safe_float(data.get("total_volume")) or safe_float(data.get("volume")) or 0.0

        now_bkk = datetime.now(BANGKOK_TZ)
        now_t = now_bkk.time()

        # Session guard: record ticks only inside the two continuous sessions
        if not is_tick_within_session(now_t):
            return

        ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")

        with self._cache_lock:
            prev_state = self.coalesced_buffer.get(sym) or self.last_persisted_state.get(sym) or self.latest_market_state.get(sym)
            if last_p is None and prev_state and prev_state.get("last") is not None:
                last_p = prev_state["last"]
            if vol == 0.0 and prev_state and prev_state.get("volume"):
                vol = prev_state["volume"]

            current_tick = {
                "symbol": sym,
                "bid": bid1 if (bid1 is not None and bid1 > 0) else None,
                "ask": ask1 if (ask1 is not None and ask1 > 0) else None,
                "bid_vol": bid_vol1,
                "ask_vol": ask_vol1,
                "last": last_p,
                "volume": vol,
                "captured_at_bkk": ts_str,
                "updated_at": pytime.time()
            }

            self.last_tick_time = pytime.time()
            self.cdc_stats["received"] += 1
            self.bid_offer_cache[sym] = current_tick
            self.latest_market_state[sym] = current_tick

            if is_tick_changed(prev_state, current_tick):
                self.cdc_stats["changed"] += 1
                # Coalescing: keep only latest state per symbol in this 3s window
                self.coalesced_buffer[sym] = current_tick
            else:
                self.cdc_stats["deduped"] += 1

        if self.debug_log_symbol and sym == self.debug_log_symbol:
            logger.info(f"[REALTIME STREAM] {sym}: Bid={bid1} (Vol={bid_vol1}), Ask={ask1} (Vol={ask_vol1})")

    def subscribe_symbols(self, symbols: List[str], max_per_conn: int = 35) -> None:
        """
        Subscribes to all symbols across a pool of RealtimeDataConnection instances.
        Each connection handles <= max_per_conn (default 35) topics, ensuring
        Settrade's per-connection limit (40) is NEVER exceeded while covering 100% of all strikes.
        Uses staggered handshake (250ms delay) to prevent connection storms.
        """
        if not self.investor:
            return

        with self._cache_lock:
            new_symbols = [s for s in symbols if s not in self.subscribed_topics]

        if not new_symbols:
            return

        chunks = [new_symbols[i:i + max_per_conn] for i in range(0, len(new_symbols), max_per_conn)]
        logger.info(
            f"[REALTIME POOL] Subscribing {len(new_symbols)} symbols across {len(chunks)} connection(s) "
            f"(Total tracked: {len(self.subscribed_topics) + len(new_symbols)})..."
        )

        for chunk_idx, chunk in enumerate(chunks):
            if chunk_idx > 0:
                pytime.sleep(0.25)  # Staggered connect to avoid broker connection storms
            try:
                rt_conn = self.investor.RealtimeDataConnection()
                with self._cache_lock:
                    self.realtime_connections.append(rt_conn)
                for sym in chunk:
                    try:
                        sub = rt_conn.subscribe_bid_offer(sym, self.on_bid_offer)
                        sub.start()
                        with self._cache_lock:
                            self.subscribed_topics.add(sym)
                    except Exception as sym_err:
                        logger.debug(f"[REALTIME] Subscription error for {sym}: {sym_err}")
                logger.info(f"[REALTIME POOL] Connection #{len(self.realtime_connections)} streaming {len(chunk)} symbols.")
            except Exception as conn_err:
                logger.warning(f"[REALTIME POOL] Failed to create connection for chunk #{chunk_idx}: {conn_err}")

    def flush_coalesced_buffer(self, turso_pipeline_url: str, turso_token: str) -> int:
        """
        Flushes coalesced changes to Turso in a single batch.
        Caps writes at maximum 1 row per symbol per 3-second cycle.
        """
        with self._cache_lock:
            if not self.coalesced_buffer:
                return 0
            items_to_flush = list(self.coalesced_buffer.values())
            self.coalesced_buffer.clear()
            for it in items_to_flush:
                self.last_persisted_state[it["symbol"]] = it

        futures_rows = []
        options_rows = []
        for it in items_to_flush:
            sym = it["symbol"]
            if sym.startswith("S50") and ("C" in sym[6:] or "P" in sym[6:]):
                m = re.match(r"^S50([A-Z]\d{2})([CP])(\d+)$", sym)
                if m:
                    ser, o_type, k_str = m.groups()
                    options_rows.append({
                        "captured_at_bkk": it["captured_at_bkk"],
                        "symbol": sym,
                        "series": ser,
                        "strike": float(k_str),
                        "option_type": o_type,
                        "bid": it["bid"],
                        "ask": it["ask"],
                        "bid_vol": it["bid_vol"],
                        "ask_vol": it["ask_vol"],
                        "last": it["last"],
                        "volume": it["volume"],
                        "open_interest": it.get("open_interest")
                    })
                else:
                    options_rows.append({
                        "captured_at_bkk": it["captured_at_bkk"],
                        "symbol": sym,
                        "series": sym[3:6],
                        "strike": 0.0,
                        "option_type": "C",
                        "bid": it["bid"],
                        "ask": it["ask"],
                        "bid_vol": it["bid_vol"],
                        "ask_vol": it["ask_vol"],
                        "last": it["last"],
                        "volume": it["volume"],
                        "open_interest": it.get("open_interest")
                    })
            else:
                futures_rows.append({
                    "captured_at_bkk": it["captured_at_bkk"],
                    "symbol": sym,
                    "bid": it["bid"],
                    "ask": it["ask"],
                    "bid_vol": it["bid_vol"],
                    "ask_vol": it["ask_vol"],
                    "last": it["last"],
                    "volume": it["volume"],
                    "open_interest": it.get("open_interest")
                })

        save_cdc_batch_to_turso(turso_pipeline_url, turso_token, futures_rows, options_rows)
        with self._cache_lock:
            self.cdc_stats["flushes"] += 1
            self.cdc_stats["rows_written"] += len(items_to_flush)
        return len(items_to_flush)

    def capture_baseline_snapshot(
        self,
        turso_pipeline_url: str,
        turso_token: str,
        checkpoint_name: str
    ) -> Tuple[int, int]:
        """
        Generates 100% of the baseline snapshot directly from in-memory state.
        Zero individual REST calls for options, staying 100% within Settrade rate limits!
        """
        now_bkk = datetime.now(BANGKOK_TZ)
        ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")

        with self._cache_lock:
            cached_symbols = list(self.latest_market_state.values())

        if not cached_symbols:
            logger.warning(f"[BASELINE SNAPSHOT] No symbols in cache for checkpoint {checkpoint_name}.")
            return 0, 0

        futures_rows = []
        options_rows = []
        for it in cached_symbols:
            sym = it["symbol"]
            row = dict(it)
            row["captured_at_bkk"] = ts_str
            if sym.startswith("S50") and ("C" in sym[6:] or "P" in sym[6:]):
                m = re.match(r"^S50([A-Z]\d{2})([CP])(\d+)$", sym)
                if m:
                    ser, o_type, k_str = m.groups()
                    options_rows.append({
                        "captured_at_bkk": ts_str,
                        "symbol": sym,
                        "series": ser,
                        "strike": float(k_str),
                        "option_type": o_type,
                        "bid": row["bid"],
                        "ask": row["ask"],
                        "bid_vol": row.get("bid_vol", 0.0),
                        "ask_vol": row.get("ask_vol", 0.0),
                        "last": row.get("last"),
                        "volume": row.get("volume", 0.0),
                        "open_interest": row.get("open_interest")
                    })
            else:
                futures_rows.append({
                    "captured_at_bkk": ts_str,
                    "symbol": sym,
                    "bid": row["bid"],
                    "ask": row["ask"],
                    "bid_vol": row.get("bid_vol", 0.0),
                    "ask_vol": row.get("ask_vol", 0.0),
                    "last": row.get("last"),
                    "volume": row.get("volume", 0.0),
                    "open_interest": row.get("open_interest")
                })

        save_cdc_batch_to_turso(turso_pipeline_url, turso_token, futures_rows, options_rows)
        with self._cache_lock:
            for row in cached_symbols:
                self.last_persisted_state[row["symbol"]] = row
            self.cdc_stats["rows_written"] += (len(futures_rows) + len(options_rows))

        logger.info(
            f"[BASELINE SNAPSHOT] ✅ Checkpoint {checkpoint_name} captured at {ts_str}: "
            f"{len(futures_rows)} futures, {len(options_rows)} options written to Turso."
        )
        return len(futures_rows), len(options_rows)

    def check_watchdog_health(self) -> Tuple[bool, float]:
        """Checks if WebSocket ticks are flowing. Returns (is_healthy, elapsed_seconds)."""
        with self._cache_lock:
            elapsed = pytime.time() - self.last_tick_time
        return (elapsed <= 15.0), elapsed

    def get_bid_offer(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        with self._cache_lock:
            entry = self.bid_offer_cache.get(symbol)
            if entry:
                return entry.get("bid"), entry.get("ask")
            return None, None

    def ensure_authenticated(self, max_age_seconds: float = 7200.0) -> None:
        """Proactively re-authenticates if token age exceeds max_age_seconds (default 2 hours)."""
        if self.investor is None or (pytime.time() - self.last_auth_time) > max_age_seconds:
            logger.info("[SETTRADE] Token age threshold reached or uninitialized. Re-authenticating...")
            self.connect()

    def handle_auth_failure(self) -> None:
        """Forced re-authentication on auth-related exceptions."""
        logger.warning("[SETTRADE] Authentication error encountered. Re-authenticating immediately...")
        self.connect()


# ==============================================================================
# 7. Worker Task for Option Quote Fetching
# ==============================================================================
def fetch_option_quote_task(
    market_data: Any,
    symbol: str,
    series: str,
    strike: float,
    opt_type: str,
    timestamp_bkk_str: str,
    limiter: Optional[PacedRateLimiter] = None,
    settrade_mgr: Optional[SettradeClientManager] = None
) -> Optional[Dict[str, Any]]:
    if limiter:
        limiter.acquire()

    try:
        quote = market_data.get_quote_symbol(symbol)
        if quote:
            fields = extract_derivatives_data(quote)
            # Enrich with live streaming Bid/Ask if available
            if settrade_mgr:
                rt_bid, rt_ask = settrade_mgr.get_bid_offer(symbol)
                if rt_bid is not None:
                    fields["bid"] = rt_bid
                if rt_ask is not None:
                    fields["ask"] = rt_ask

            return {
                "captured_at_bkk": timestamp_bkk_str,
                "symbol": symbol,
                "series": series,
                "strike": strike,
                "option_type": opt_type,
                "bid": fields["bid"],
                "ask": fields["ask"],
                "last": fields["last"],
                "volume": fields["volume"],
                "open_interest": fields["open_interest"]
            }
    except SettradeError:
        # Unlisted strike / instrument not traded today: expected, not a failure.
        return None
    except Exception as e:
        logger.warning(f"Option fetch error for {symbol}: {type(e).__name__}: {e}")
        return None
    return None


def collect_and_save_snapshot(
    market_data: Any,
    turso_pipeline_url: str,
    turso_token: str,
    workers: int,
    max_rps: float,
    timestamp_bkk_str: str,
    now_bkk: datetime,
    front_opt_series: str,
    next_opt_series: str,
    futures_candidates: List[str],
    settrade_mgr: Optional[SettradeClientManager] = None
) -> Tuple[int, int, int]:
    """
    Performs atomic 1-minute market snapshot collection (Spot, Futures, Options)
    and commits to Turso Cloud SQLite.
    Returns: (spot_count, futures_count, options_count)
    """
    spot_snapshots: List[Dict[str, Any]] = []
    futures_snapshots: List[Dict[str, Any]] = []
    options_snapshots: List[Dict[str, Any]] = []

    # 1. Fetch SET50 Spot Index
    spot_price_ref: Optional[float] = None
    try:
        try:
            spot_res = market_data.get_quote_symbol("SET50")
            spot_fields = extract_spot_data(spot_res)
        except Exception:
            candles = market_data.get_candlestick("SET50", interval="1m", limit=1)
            spot_fields = {
                "last": float(candles["close"][-1]) if candles.get("close") else None,
                "volume": float(candles["volume"][-1]) if candles.get("volume") else 0.0,
                "value": float(candles["value"][-1]) if candles.get("value") else 0.0,
            }

        spot_price_ref = spot_fields["last"]
        spot_snapshots.append({
            "captured_at_bkk": timestamp_bkk_str,
            "symbol": "SET50",
            "last": spot_fields["last"],
            "volume": spot_fields["volume"],
            "value": spot_fields["value"]
        })
        logger.info(f"SET50 Spot captured: Last={spot_fields['last']}, Volume={spot_fields['volume']}, Value={spot_fields['value']}")
    except Exception as e:
        logger.warning(f"Failed to fetch SET50 Spot: {e}")

    # 2. Discover and Fetch Active S50 Futures
    front_futures_price: Optional[float] = None
    for series_tag in futures_candidates:
        symbol = f"S50{series_tag}"
        try:
            quote = market_data.get_quote_symbol(symbol)
            if quote:
                fields = extract_derivatives_data(quote)
                if settrade_mgr:
                    rt_bid, rt_ask = settrade_mgr.get_bid_offer(symbol)
                    if rt_bid is not None:
                        fields["bid"] = rt_bid
                    if rt_ask is not None:
                        fields["ask"] = rt_ask

                futures_snapshots.append({
                    "captured_at_bkk": timestamp_bkk_str,
                    "symbol": symbol,
                    "bid": fields["bid"],
                    "ask": fields["ask"],
                    "last": fields["last"],
                    "volume": fields["volume"],
                    "open_interest": fields["open_interest"]
                })
                logger.info(
                    f"Futures [{symbol}] captured: Last={fields['last']}, "
                    f"Bid={fields['bid']}, Ask={fields['ask']}, Vol={fields['volume']}, OI={fields['open_interest']}"
                )
                if front_futures_price is None and fields["last"] is not None:
                    front_futures_price = fields["last"]
        except SettradeError:
            logger.debug(f"Candidate futures symbol {symbol} is not active in market.")
        except Exception as e:
            logger.warning(f"Error querying futures symbol {symbol}: {e}")

    # 3. Fetch SET50 Options for All Active Strikes (Front & Next Series)
    atm_base = spot_price_ref or front_futures_price or DEFAULT_ATM_BASE
    all_strikes = generate_all_active_strikes(atm_base)

    # Subscribe ALL candidate symbols across the entire board to Realtime Streaming Pool
    if settrade_mgr:
        all_stream_symbols = generate_all_stream_symbols(
            front_series=front_opt_series,
            next_series=next_opt_series,
            futures_candidates=futures_candidates,
            all_strikes=all_strikes
        )
        settrade_mgr.subscribe_symbols(all_stream_symbols)

    option_targets: List[Tuple[str, str, float, str]] = []
    for opt_series in [front_opt_series, next_opt_series]:
        for strike in all_strikes:
            for opt_type in ["C", "P"]:
                symbol = f"S50{opt_series}{opt_type}{strike}"
                option_targets.append((symbol, opt_series, float(strike), opt_type))

    pacer = PacedRateLimiter(max_rps=max_rps)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures_tasks = [
            executor.submit(
                fetch_option_quote_task,
                market_data,
                sym,
                ser,
                stk,
                otp,
                timestamp_bkk_str,
                pacer,
                settrade_mgr
            )
            for (sym, ser, stk, otp) in option_targets
        ]

        for future in as_completed(futures_tasks):
            result = future.result()
            if result is not None:
                options_snapshots.append(result)

    total_records = len(spot_snapshots) + len(futures_snapshots) + len(options_snapshots)
    logger.info(
        f"Snapshot summary at {timestamp_bkk_str}: Spot={len(spot_snapshots)}, "
        f"Futures={len(futures_snapshots)}, Options={len(options_snapshots)} (Total={total_records})"
    )

    if total_records == 0:
        logger.warning(f"No records collected during tick at {timestamp_bkk_str}. Skipping Turso write.")
        raise RuntimeError(f"Zero records collected from Settrade Open API at {timestamp_bkk_str} (Possible API Outage/Timeout)")

    # 4. Write to Turso via HTTP Pipeline
    payload = build_turso_pipeline_payload(spot_snapshots, futures_snapshots, options_snapshots)
    execute_turso_pipeline(turso_pipeline_url, turso_token, payload)

    return len(spot_snapshots), len(futures_snapshots), len(options_snapshots)


# ==============================================================================
# 8. Pre-Market Standby & Holiday Early Exit Guards
# ==============================================================================
def wait_for_market_open(target_open_time: time, session_name: str) -> None:
    """
    If script is awakened before session market open (e.g. 09:15 or 13:45),
    logs standby status and sleeps precisely until target_open_time (:00 seconds).
    """
    now_bkk = datetime.now(BANGKOK_TZ)
    target_dt = now_bkk.replace(
        hour=target_open_time.hour,
        minute=target_open_time.minute,
        second=0,
        microsecond=0
    )
    if target_dt <= now_bkk:
        return

    total_wait_sec = (target_dt - now_bkk).total_seconds()
    logger.info(
        f"[SESSION] {session_name} worker awakened in pre-market standby window at {now_bkk.strftime('%H:%M:%S')}. "
        f"Waiting until market open at {target_open_time.strftime('%H:%M:%S')} ({total_wait_sec:.0f}s)..."
    )

    while True:
        current_bkk = datetime.now(BANGKOK_TZ)
        rem = (target_dt - current_bkk).total_seconds()
        if rem <= 0.05:
            break
        if rem > 60:
            mins_left = int(rem // 60)
            secs_left = int(rem % 60)
            logger.info(f"[PRE-MARKET] Standby for {session_name} open ({target_open_time.strftime('%H:%M:%S')})... {mins_left}m {secs_left}s remaining")
            pytime.sleep(min(rem - 1.0, 60.0))
        else:
            pytime.sleep(rem)
            break

    logger.info(f"[SESSION] Market open time reached ({datetime.now(BANGKOK_TZ).strftime('%H:%M:%S')}). Commencing 1-minute collection loop.")


def check_holiday_early_exit(market_data: Any, front_opt_series: str, now_bkk: datetime) -> None:
    """
    10 minutes past market open (09:55 or 13:55), checks whether today's trading
    activity has started on Front Futures or SET50. If no today candles exist, exits gracefully (sys.exit(0)).
    """
    front_fut_symbol = f"S50{front_opt_series}"
    logger.info(f"[GUARD] Verifying live market activity on {front_fut_symbol} (10 min past market open)...")
    try:
        candles = market_data.get_candlestick(front_fut_symbol, interval="1m", limit=1)
        today_date = now_bkk.date()
        if candles and "time" in candles and candles["time"]:
            latest_ts = candles["time"][-1]
            candle_dt = datetime.fromtimestamp(latest_ts, BANGKOK_TZ)
            if candle_dt.date() < today_date:
                # Cross-check SET50 spot index
                spot_has_today = False
                try:
                    spot_candles = market_data.get_candlestick("SET50", interval="1m", limit=1)
                    if spot_candles and "time" in spot_candles and spot_candles["time"]:
                        s_dt = datetime.fromtimestamp(spot_candles["time"][-1], BANGKOK_TZ)
                        if s_dt.date() == today_date:
                            spot_has_today = True
                except Exception:
                    pass

                if not spot_has_today:
                    # Exit 3, not 0: the calendar already ruled out a scheduled
                    # holiday, so "no trades today" is either an unscheduled
                    # closure or a data outage. Both need a human to look, and a
                    # zero exit reported the run as successful either way.
                    logger.error(
                        f"[GUARD] Market inactive 10 minutes after open ({now_bkk.strftime('%H:%M:%S')}): "
                        f"latest candle for {front_fut_symbol} is {candle_dt.date()} (< {today_date}) and "
                        f"SET50 has no candle today. Exiting with code 3 (unscheduled closure or data outage)."
                    )
                    sys.exit(3)

            logger.info(
                f"[GUARD] Live market verified: Latest 1m candle for {front_fut_symbol} is "
                f"{candle_dt.strftime('%Y-%m-%d %H:%M')}. Market is ACTIVE."
            )
    except Exception as e:
        logger.warning(f"[GUARD] Could not verify market activity via {front_fut_symbol}: {e}")


# ==============================================================================
# 9. Execution Modes: Session Worker Loop, Single Snapshot, Manual Notify
# ==============================================================================
def fetch_spot_snapshot(market_data: Any, timestamp_bkk_str: str) -> Optional[Dict[str, Any]]:
    try:
        spot_fields = None
        try:
            spot_res = market_data.get_quote_symbol("SET50")
            if spot_res:
                extracted = extract_spot_data(spot_res)
                if extracted.get("last") is not None:
                    spot_fields = extracted
        except Exception:
            pass

        if spot_fields is None:
            candles = market_data.get_candlestick("SET50", interval="1m", limit=1)
            spot_fields = {
                "last": float(candles["close"][-1]) if candles.get("close") else None,
                "volume": float(candles["volume"][-1]) if candles.get("volume") else 0.0,
                "value": float(candles["value"][-1]) if candles.get("value") else 0.0,
            }
        return {
            "captured_at_bkk": timestamp_bkk_str,
            "symbol": "SET50",
            "last": spot_fields["last"],
            "volume": spot_fields["volume"],
            "value": spot_fields["value"]
        }
    except Exception as e:
        logger.warning(f"[SPOT] Failed to fetch SET50 Spot: {e}")
        return None


def run_session_worker(
    settrade_mgr: SettradeClientManager,
    turso_pipeline_url: str,
    turso_token: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    workers: int,
    max_rps: float,
    force: bool = False
) -> None:
    now_bkk = datetime.now(BANGKOK_TZ)
    today_str = now_bkk.strftime("%Y-%m-%d")

    # 1. Calendar Holiday & Weekend Checks
    if not force:
        if now_bkk.weekday() > 4:
            logger.info(f"[GUARD] Weekend ({now_bkk.strftime('%A')}). Exiting gracefully (sys.exit(0)).")
            sys.exit(0)
        if today_str in SET_HOLIDAYS:
            logger.info(f"[GUARD] Today ({today_str}) is an official SET/TFEX exchange holiday. Exiting gracefully (sys.exit(0)).")
            sys.exit(0)

    # Concurrency Guard: Check if another worker is already actively capturing ticks
    if not force and is_worker_already_active(turso_pipeline_url, turso_token, now_bkk):
        print("Another worker is already active. Exiting cleanly.")
        logger.info("[GUARD] Another worker is already active. Exiting cleanly (sys.exit(0)).")
        sys.exit(0)

    # 2. Detect Session Window
    session_cfg = detect_session_window(now_bkk)
    if not session_cfg and not force:
        logger.info(
            f"[GUARD] Current time {now_bkk.strftime('%H:%M:%S')} is outside active session windows "
            f"(Morning: 09:00 - 12:30, Afternoon: 13:00 - 16:55). Exiting gracefully (sys.exit(0))."
        )
        sys.exit(0)

    forced_session_deadline: Optional[datetime] = None
    if session_cfg:
        session_name = session_cfg["name"]
        market_open_time = session_cfg["market_open"]
        verify_holiday_time = session_cfg["verify_holiday"]
        session_end_time = session_cfg["session_end"]
    else:
        # Force mode outside scheduled session windows
        session_name = "FORCED_AD_HOC"
        market_open_time = now_bkk.time()
        verify_holiday_time = time(23, 59, 59)
        session_end_time = (now_bkk + timedelta(minutes=10)).time()
        # A .time() comparison wraps at midnight; compare datetimes instead.
        forced_session_deadline = now_bkk + timedelta(minutes=10)
        logger.info(
        f"[SESSION] Running forced ad-hoc session until "
        f"{(forced_session_deadline or now_bkk).strftime('%Y-%m-%d %H:%M:%S')}"
    )

    logger.info(f"=== Starting TFEX Persistent Session Worker: {session_name} ===")
    logger.info(f"Market Open: {market_open_time.strftime('%H:%M:%S')} | Session End: {session_end_time.strftime('%H:%M:%S')}")

    # Clear in-memory caches from previous runs at initial startup
    settrade_mgr.clear_session_cache()

    # 3. Connect to Settrade Open API
    try:
        settrade_mgr.connect()
    except Exception as e:
        logger.error(f"Cannot initialize Settrade Open API connection: {e}")
        sys.exit(1)

    # 4. Pre-warm: Ensure schema, resolve contracts, and subscribe ALL symbols to Realtime Pool
    # Pre-warming runs immediately (before 09:44:30 / 13:44:30) to guarantee the WebSocket pool is 100% connected
    now_bkk = datetime.now(BANGKOK_TZ)
    ensure_database_schema(turso_pipeline_url, turso_token)
    futures_candidates, front_opt_series, next_opt_series = resolve_active_series_candidates(now_bkk)
    front_fut_symbol = f"S50{front_opt_series}"
    settrade_mgr.debug_log_symbol = front_fut_symbol
    logger.info(f"[SESSION] Active Series: Front={front_opt_series} ({front_fut_symbol}), Next={next_opt_series}, Futures={futures_candidates}")

    all_strikes = generate_all_active_strikes(DEFAULT_ATM_BASE)
    all_symbols = generate_all_stream_symbols(
        front_series=front_opt_series,
        next_series=next_opt_series,
        futures_candidates=futures_candidates,
        all_strikes=all_strikes
    )
    logger.info(f"[PRE-WARM] Subscribing {len(all_symbols)} symbols across WebSocket pool before market open...")
    settrade_mgr.subscribe_symbols(all_symbols)

    stop_event = threading.Event()

    # 3-Second Micro-Batch Coalescing Flush Thread
    def cdc_flush_loop():
        logger.info("[CDC FLUSH] Background micro-batch flush thread started (3.0s cycle).")
        consecutive_errors = 0
        while not stop_event.is_set():
            try:
                flushed = settrade_mgr.flush_coalesced_buffer(turso_pipeline_url, turso_token)
                consecutive_errors = 0
                if flushed > 0:
                    logger.debug(f"[CDC FLUSH] Flushed {flushed} coalesced changes to Turso.")
            except Exception as fe:
                consecutive_errors += 1
                logger.warning(f"[CDC FLUSH] Error flushing buffer to Turso (attempt {consecutive_errors}): {fe}")
                if consecutive_errors >= 3:
                    send_critical_error_alert(
                        telegram_bot_token,
                        telegram_chat_id,
                        error_title="Turso CDC Batch Flush Failed 3+ Consecutive Times",
                        error_detail=str(fe)
                    )
            stop_event.wait(3.0)
        logger.info("[CDC FLUSH] Background micro-batch flush thread exiting.")

    # 15-Second Active Watchdog Thread
    def cdc_watchdog_loop():
        logger.info("[CDC WATCHDOG] Active watchdog thread started (15.0s cycle).")
        while not stop_event.is_set():
            stop_event.wait(15.0)
            if stop_event.is_set():
                break
            now_t = datetime.now(BANGKOK_TZ).time()
            in_market = (time(9, 45) <= now_t <= time(12, 30)) or (time(13, 45) <= now_t <= time(16, 55))
            if in_market:
                is_healthy, elapsed = settrade_mgr.check_watchdog_health()
                if not is_healthy:
                    logger.warning(f"[WATCHDOG] ⚠️ WebSocket silence detected! No tick received for {elapsed:.1f}s during market hours.")
                    if elapsed > 30.0:
                        logger.error(f"[WATCHDOG] Stream stalled for {elapsed:.1f}s (>30s). Triggering auto-reconnect of Realtime Pool...")
                        try:
                            settrade_mgr.stop_all_realtime()
                            settrade_mgr.ensure_authenticated()
                            settrade_mgr.subscribe_symbols(all_symbols)
                            logger.info("[WATCHDOG] Realtime Pool auto-reconnect complete.")
                        except Exception as recon_err:
                            logger.error(f"[WATCHDOG] Reconnect error: {recon_err}")
                            send_critical_error_alert(
                                telegram_bot_token,
                                telegram_chat_id,
                                error_title="Settrade WebSocket Watchdog Auto-Reconnect Failed",
                                error_detail=str(recon_err)
                            )
        logger.info("[CDC WATCHDOG] Active watchdog thread exiting.")

    flush_thread = threading.Thread(target=cdc_flush_loop, daemon=True, name="CDCFlushThread")
    watchdog_thread = threading.Thread(target=cdc_watchdog_loop, daemon=True, name="CDCWatchdogThread")
    flush_thread.start()
    watchdog_thread.start()

    # 5. Pre-Market Standby: Wait precisely until market open time (:00 seconds)
    now_bkk = datetime.now(BANGKOK_TZ)
    if not force and now_bkk.time() < market_open_time:
        logger.info(
            f"[PRE-WARM] Pre-warm complete before {market_open_time.strftime('%H:%M:%S')}. "
            f"WebSocket pool active ({len(settrade_mgr.realtime_connections)} conns, {len(settrade_mgr.subscribed_topics)} topics). "
            f"Standing by for market open..."
        )
        wait_for_market_open(market_open_time, session_name)
        # Note: Do NOT clear_session_cache() here so pre-warmed state is preserved for baseline!

    # Re-check Concurrency Guard after market open standby
    now_bkk = datetime.now(BANGKOK_TZ)
    if not force and is_worker_already_active(turso_pipeline_url, turso_token, now_bkk):
        print("Another worker is already active. Exiting cleanly.")
        logger.info("[GUARD] Another worker is already active. Exiting cleanly (sys.exit(0)).")
        sys.exit(0)

    market_verified = False
    session_error_count = 0
    triggered_baselines: Set[str] = set()
    last_spot_minute = -1

    # 6. Session Worker Loop (1-second precision loop)
    logger.info(f"--- [CDC LOOP] Realtime Event Capture Loop Engaged (Session: {session_name}) ---")
    while True:
        loop_now = datetime.now(BANGKOK_TZ)
        loop_time = loop_now.time()

        # Check cutoff / graceful exit condition (12:30:05 morning, 16:55:05 afternoon)
        if not force:
            if session_name == "MORNING" and loop_time >= time(12, 30, 5):
                logger.info("[SESSION] Reached morning cutoff time (12:30:05). Stopping capture.")
                break
            elif session_name == "AFTERNOON" and loop_time >= time(16, 55, 5):
                logger.info("[SESSION] Reached afternoon cutoff time (16:55:05). Stopping capture.")
                break
            elif forced_session_deadline is not None and loop_now >= forced_session_deadline:
                logger.info(
                    f"[SESSION] Reached forced session deadline "
                    f"({forced_session_deadline.strftime('%Y-%m-%d %H:%M:%S')}). Exiting gracefully."
                )
                break
            elif session_cfg is not None and loop_time >= session_end_time:
                logger.info(f"[SESSION] Reached session end time ({session_end_time.strftime('%H:%M:%S')}). Exiting gracefully.")
                break

        # Proactive token re-auth check
        try:
            settrade_mgr.ensure_authenticated()
        except Exception as e:
            logger.warning(f"[SETTRADE] Proactive auth refresh warning: {e}")

        # 1-minute Spot & Front Futures Last Price capture (:00 second of every minute)
        if loop_now.minute != last_spot_minute and loop_now.second >= 0:
            last_spot_minute = loop_now.minute
            ts_str = loop_now.strftime("%Y-%m-%d %H:%M:%S")
            try:
                spot_row = fetch_spot_snapshot(settrade_mgr.market_data, ts_str)
                if spot_row:
                    save_cdc_batch_to_turso(turso_pipeline_url, turso_token, futures_rows=[], options_rows=[], spot_rows=[spot_row])
            except Exception as spot_err:
                session_error_count += 1
                logger.warning(f"[SPOT ERROR] Could not save SET50 Spot at {ts_str}: {spot_err}")

            # Also refresh Front Month Futures last price & volume from quote
            try:
                fut_quote = settrade_mgr.market_data.get_quote_symbol(front_fut_symbol)
                if fut_quote:
                    fut_fields = extract_derivatives_data(fut_quote)
                    if fut_fields.get("last") is not None:
                        with settrade_mgr._cache_lock:
                            st = settrade_mgr.latest_market_state.get(front_fut_symbol, {
                                "symbol": front_fut_symbol,
                                "captured_at_bkk": ts_str,
                                "updated_at": pytime.time()
                            })
                            st["last"] = fut_fields["last"]
                            if fut_fields.get("volume"):
                                st["volume"] = fut_fields["volume"]
                            if fut_fields.get("open_interest"):
                                st["open_interest"] = fut_fields["open_interest"]
                            settrade_mgr.latest_market_state[front_fut_symbol] = st

                            if front_fut_symbol in settrade_mgr.coalesced_buffer:
                                settrade_mgr.coalesced_buffer[front_fut_symbol]["last"] = fut_fields["last"]
                                if fut_fields.get("volume"):
                                    settrade_mgr.coalesced_buffer[front_fut_symbol]["volume"] = fut_fields["volume"]
                            else:
                                settrade_mgr.coalesced_buffer[front_fut_symbol] = dict(st)
                                settrade_mgr.cdc_stats["changed"] += 1
            except Exception as fut_err:
                logger.debug(f"[FUTURES QUOTE] Could not refresh {front_fut_symbol} quote: {fut_err}")

        # Baseline Snapshot Trigger (09:45, 12:30, 13:45, 16:55 + 30-min heartbeats)
        h, m = loop_time.hour, loop_time.minute
        ts_minute_key = f"{loop_now.strftime('%Y%m%d')}_{h:02d}{m:02d}"
        if ts_minute_key not in triggered_baselines:
            checkpoint_name = None
            if h == 9 and m == 45:
                checkpoint_name = "09:45_MORNING_OPEN"
            elif h == 12 and m == 30:
                checkpoint_name = "12:30_MIDDAY_CLOSE"
            elif h == 13 and m == 45:
                checkpoint_name = "13:45_AFTERNOON_OPEN"
            elif h == 16 and m == 55:
                checkpoint_name = "16:55_EVENING_CLOSE"
            elif m in (15, 45):
                in_market = (time(9, 45) <= loop_time <= time(12, 30)) or (time(13, 45) <= loop_time <= time(16, 55))
                if in_market:
                    checkpoint_name = f"{h:02d}:{m:02d}_HEARTBEAT_30M"

            if checkpoint_name:
                triggered_baselines.add(ts_minute_key)
                try:
                    f_c, o_c = settrade_mgr.capture_baseline_snapshot(
                        turso_pipeline_url, turso_token, checkpoint_name
                    )
                    logger.info(f"[BASELINE] Saved baseline {checkpoint_name}: {f_c} fut, {o_c} opt.")
                except Exception as b_err:
                    session_error_count += 1
                    logger.error(f"[BASELINE ERROR] Failed to save baseline snapshot {checkpoint_name}: {b_err}")

        # 10-minute Holiday / Inactive market check
        if not market_verified and not force and loop_time >= verify_holiday_time:
            check_holiday_early_exit(settrade_mgr.market_data, front_opt_series, loop_now)
            market_verified = True

        # Telegram 4-checkpoint alerts evaluation (10:00, 12:30, 14:00, 16:55)
        if telegram_bot_token and telegram_chat_id:
            try:
                check_and_send_checkpoints(
                    loop_now, turso_pipeline_url, turso_token,
                    telegram_bot_token, telegram_chat_id, force_notify=False,
                    session_errors=session_error_count,
                    cdc_stats=settrade_mgr.cdc_stats
                )
            except Exception as e:
                logger.warning(f"[TELEGRAM] Checkpoint alert error: {e}")

        # Sleep until next second
        pytime.sleep(1.0)

    # Teardown
    stop_event.set()
    flush_thread.join(timeout=5.0)
    watchdog_thread.join(timeout=5.0)
    try:
        settrade_mgr.flush_coalesced_buffer(turso_pipeline_url, turso_token)
    except Exception as fe:
        logger.warning(f"[TEARDOWN] Final flush error: {fe}")
    settrade_mgr.stop_all_realtime()

    # Trigger final session checkpoint alert (MIDDAY_CLOSE or EVENING_CLOSE) if configured
    if telegram_bot_token and telegram_chat_id:
        try:
            check_and_send_checkpoints(
                datetime.now(BANGKOK_TZ), turso_pipeline_url, turso_token,
                telegram_bot_token, telegram_chat_id, force_notify=False,
                session_errors=session_error_count,
                cdc_stats=settrade_mgr.cdc_stats
            )
        except Exception as e:
            logger.warning(f"[TELEGRAM] Teardown checkpoint alert error: {e}")

    logger.info(
        f"=== Session Worker Completed Successfully: {session_name} ===\n"
        f"CDC Stats: {settrade_mgr.cdc_stats}\n"
        f"Exit time: {datetime.now(BANGKOK_TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    sys.exit(0)


def run_single_snapshot(
    settrade_mgr: SettradeClientManager,
    turso_pipeline_url: str,
    turso_token: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    workers: int,
    max_rps: float,
    force: bool = False
) -> None:
    now_bkk = datetime.now(BANGKOK_TZ)
    today_str = now_bkk.strftime("%Y-%m-%d")
    ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")

    if not force:
        if today_str in SET_HOLIDAYS:
            logger.info(f"[GUARD] Today ({today_str}) is an official SET/TFEX exchange holiday. Exiting (sys.exit(0)).")
            sys.exit(0)
        within_hours, session_desc = is_within_trading_hours(now_bkk)
        if not within_hours:
            logger.info(f"[GUARD] Current time is outside TFEX trading sessions ({session_desc}). Exiting (sys.exit(0)).")
            sys.exit(0)

    try:
        settrade_mgr.connect()
    except Exception as e:
        logger.error(f"Cannot initialize Settrade Open API connection: {e}")
        sys.exit(1)
    futures_candidates, front_opt_series, next_opt_series = resolve_active_series_candidates(now_bkk)

    # Pre-subscribe ALL symbols across the entire board to Realtime Streaming Pool
    all_strikes = generate_all_active_strikes(DEFAULT_ATM_BASE)
    all_symbols = generate_all_stream_symbols(
        front_series=front_opt_series,
        next_series=next_opt_series,
        futures_candidates=futures_candidates,
        all_strikes=all_strikes
    )
    settrade_mgr.subscribe_symbols(all_symbols)
    logger.info("[REALTIME] Waiting 2.0s for initial streaming quotes to arrive...")
    pytime.sleep(2.0)

    spot_c, fut_c, opt_c = collect_and_save_snapshot(
        market_data=settrade_mgr.market_data,
        turso_pipeline_url=turso_pipeline_url,
        turso_token=turso_token,
        workers=workers,
        max_rps=max_rps,
        timestamp_bkk_str=ts_str,
        now_bkk=now_bkk,
        front_opt_series=front_opt_series,
        next_opt_series=next_opt_series,
        futures_candidates=futures_candidates,
        settrade_mgr=settrade_mgr
    )

    if telegram_bot_token and telegram_chat_id:
        try:
            check_and_send_checkpoints(
                now_bkk, turso_pipeline_url, turso_token,
                telegram_bot_token, telegram_chat_id, force_notify=False
            )
        except Exception as e:
            logger.warning(f"[TELEGRAM] Checkpoint evaluation error: {e}")

    logger.info(f"Single snapshot completed: Spot={spot_c}, Futures={fut_c}, Options={opt_c}")
    sys.exit(0)


def run_notify(
    turso_pipeline_url: str,
    turso_token: str,
    telegram_bot_token: str,
    telegram_chat_id: str
) -> None:
    now_bkk = datetime.now(BANGKOK_TZ)
    logger.info("[NOTIFY] Executing manual Telegram status report...")
    success = check_and_send_checkpoints(
        now_bkk, turso_pipeline_url, turso_token,
        telegram_bot_token, telegram_chat_id, force_notify=True
    )
    sys.exit(0 if success else 1)


def load_dotenv_simple(env_path: str = ".env") -> None:
    """Loads environment variables from .env file if present."""
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, val = line.split("=", 1)
                        key = key.strip()
                        val = val.strip().strip("\"'")
                        if key and key not in os.environ:
                            os.environ[key] = val
            logger.info("Loaded environment variables from local .env file.")
        except Exception as e:
            logger.warning(f"Could not read .env file: {e}")


def clean_mode_guard(clean: bool, confirm_clean: bool) -> Optional[str]:
    """Reason `--clean` must be refused, or None when it is explicitly confirmed.

    --clean runs `DELETE FROM ...` with no WHERE clause against three tables;
    it used to be reachable from the workflow dispatch dropdown (one click away
    from `session`) with no confirmation step at all.
    """
    if not clean:
        return None
    if confirm_clean:
        return None
    return (
        "--clean deletes EVERY row in options_snapshots, futures_snapshots and spot_snapshots "
        "with no backup and no date filter. Re-run with --confirm-clean if that is intended."
    )


def run_clean_db(pipeline_url: str, auth_token: str) -> None:
    """
    Cleans incomplete/stale intraday snapshot data and logs from Turso Cloud SQLite.
    Preserves thor_rates and other non-intraday reference tables.
    """
    logger.info("[CLEAN] Purging stale/incomplete intraday data from Turso Cloud SQLite...")
    pre_counts = query_turso_pipeline(pipeline_url, auth_token, [
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) as cnt FROM options_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) as cnt FROM futures_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) as cnt FROM spot_snapshots;"}},
        {"type": "close"},
    ])
    try:
        pre = [
            int(parse_turso_result_rows(pre_counts[i]["response"]["result"])[0]["cnt"] or 0)
            for i in range(3)
        ]
        logger.warning(
            f"[CLEAN] About to delete ALL rows: options_snapshots={pre[0]:,}, "
            f"futures_snapshots={pre[1]:,}, spot_snapshots={pre[2]:,}"
        )
    except Exception as pre_err:
        logger.warning(f"[CLEAN] Could not pre-count rows before delete: {pre_err}")

    clean_stmts = [
        {"type": "execute", "stmt": {"sql": "DELETE FROM options_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "DELETE FROM futures_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "DELETE FROM spot_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "DELETE FROM intraday_tick_errors;"}},
        {"type": "execute", "stmt": {"sql": "DELETE FROM intraday_telegram_log;"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM options_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM futures_snapshots;"}},
        {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM spot_snapshots;"}},
    ]
    results = query_turso_pipeline(pipeline_url, auth_token, clean_stmts)

    def _post_count(idx: int) -> Any:
        """Row count after the delete, or "unknown" when the response is short."""
        if idx >= len(results):
            return "unknown"
        try:
            rows = results[idx]["response"]["result"]["rows"]
            return int(rows[0][0]["value"])
        except (KeyError, IndexError, TypeError, ValueError):
            return "unknown"

    opt_cnt, fut_cnt, spot_cnt = _post_count(5), _post_count(6), _post_count(7)
    logger.info(f"[CLEAN] Database tables cleared. Post-clean row counts: Options={opt_cnt}, Futures={fut_cnt}, Spot={spot_cnt}")
    print(f"✅ Clean complete: options_snapshots ({opt_cnt}), futures_snapshots ({fut_cnt}), spot_snapshots ({spot_cnt}). Ready for fresh start.")
    sys.exit(0)


# ==============================================================================
# 10. CLI Parser & Main Entrypoint
# ==============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="TFEX Persistent Session Worker & Market Data Collector"
    )
    parser.add_argument(
        "--session",
        action="store_true",
        help="Run persistent 1-minute session worker loop until session closes (Morning: 09:45-12:30, Afternoon: 13:45-16:55)."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single collection snapshot and exit."
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Purge all old incomplete intraday snapshot data and logs from Turso Cloud SQLite."
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Manually trigger and send the latest intraday status report to Telegram."
    )
    parser.add_argument(
        "--confirm-clean",
        action="store_true",
        help="Required together with --clean: acknowledges that --clean deletes every intraday row."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass trading hours and holiday guards."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Concurrency workers for Market Data queries (default: 8)."
    )
    parser.add_argument(
        "--max-rps",
        type=float,
        default=45.0,
        help="Maximum requests per second to avoid 429 gateway burst (default: 45.0)."
    )
    args = parser.parse_args()

    refusal = clean_mode_guard(args.clean, args.confirm_clean)
    if refusal:
        logger.error(f"[CLEAN GUARD] {refusal}")
        sys.exit(2)

    load_dotenv_simple(".env")

    # Read environment variables
    app_id = os.environ.get("SETTRADE_APP_ID", "").strip()
    app_secret = os.environ.get("SETTRADE_APP_SECRET", "").strip()
    broker_id = os.environ.get("SETTRADE_BROKER_ID", "").strip()
    app_code = os.environ.get("SETTRADE_APP_CODE", "ALGO").strip() or "ALGO"
    is_sandbox_raw = os.environ.get("SETTRADE_IS_SANDBOX", "false").strip().lower()
    is_sandbox = is_sandbox_raw in ("true", "1", "yes")

    turso_url = os.environ.get("TURSO_DATABASE_URL", "").strip()
    turso_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()
    telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    # Mode: --clean
    if args.clean:
        if not turso_url or not turso_token:
            logger.error("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN for --clean mode.")
            sys.exit(1)
        turso_pipeline_url = normalize_turso_pipeline_url(turso_url)
        run_clean_db(turso_pipeline_url, turso_token)

    # Mode: --notify
    if args.notify:
        if not turso_url or not turso_token:
            logger.error("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN for --notify mode.")
            sys.exit(1)
        turso_pipeline_url = normalize_turso_pipeline_url(turso_url)
        run_notify(turso_pipeline_url, turso_token, telegram_bot_token, telegram_chat_id)

    # Validate Settrade & Turso credentials for collection modes
    missing_env: List[str] = []
    if not app_id: missing_env.append("SETTRADE_APP_ID")
    if not app_secret: missing_env.append("SETTRADE_APP_SECRET")
    if not broker_id: missing_env.append("SETTRADE_BROKER_ID")
    if not turso_url: missing_env.append("TURSO_DATABASE_URL")
    if not turso_token: missing_env.append("TURSO_AUTH_TOKEN")

    if missing_env:
        logger.error(f"Missing required environment variables: {', '.join(missing_env)}")
        sys.exit(1)

    turso_pipeline_url = normalize_turso_pipeline_url(turso_url)
    settrade_mgr = SettradeClientManager(
        app_id=app_id,
        app_secret=app_secret,
        broker_id=broker_id,
        app_code=app_code,
        is_sandbox=is_sandbox
    )

    # Mode: --once
    if args.once:
        run_single_snapshot(
            settrade_mgr=settrade_mgr,
            turso_pipeline_url=turso_pipeline_url,
            turso_token=turso_token,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            workers=args.workers,
            max_rps=args.max_rps,
            force=args.force
        )

    # Default / Mode: --session
    run_session_worker(
        settrade_mgr=settrade_mgr,
        turso_pipeline_url=turso_pipeline_url,
        turso_token=turso_token,
        telegram_bot_token=telegram_bot_token,
        telegram_chat_id=telegram_chat_id,
        workers=args.workers,
        max_rps=args.max_rps,
        force=args.force
    )


if __name__ == "__main__":
    main()
