#!/usr/bin/env python3
"""
Stateless TFEX Market Data Collector (1-Minute Snapshot)
Fetches SET50 Spot Index, S50 Futures, and SET50 Options from Settrade Open API (v2)
and writes batch snapshots into Turso Cloud SQLite via HTTP Pipeline API (/v2/pipeline).

Optimized for high-throughput Market Data (up to 60 req/s) with connection pooling,
paced concurrency to prevent 429 bursts, and full board strike scanning.
"""

import os
import sys
import time as pytime
import argparse
import calendar
import logging
import threading
from datetime import datetime, time, date
from typing import Dict, List, Optional, Tuple, Any
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

# TFEX Month code mapping
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}
QUARTERLY_MONTHS = {3, 6, 9, 12}  # H, M, U, Z

# ==============================================================================
# SET / TFEX Official Exchange Holidays (2026 - 2027)
# ==============================================================================
SET_HOLIDAYS = {
    # 2026
    "2026-01-01",  # New Year's Day
    "2026-01-02",  # Special Holiday
    "2026-03-03",  # Makha Bucha Day
    "2026-04-06",  # Chakri Memorial Day
    "2026-04-13",  # Songkran Festival
    "2026-04-14",  # Songkran Festival
    "2026-04-15",  # Songkran Festival
    "2026-05-01",  # National Labour Day
    "2026-05-04",  # Coronation Day
    "2026-06-01",  # Substitution for Visakha Bucha Day (Sun May 31)
    "2026-06-03",  # H.M. Queen Suthida's Birthday
    "2026-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2026-07-29",  # Asahna Bucha Day
    "2026-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday / Mother's Day
    "2026-10-13",  # King Bhumibol Adulyadej Memorial Day (Navamindra Maharaj Day)
    "2026-10-16",  # Special Holiday
    "2026-10-23",  # Chulalongkorn Day
    "2026-12-07",  # Substitution for King Bhumibol's Birthday / Father's Day (Sat Dec 5)
    "2026-12-10",  # Constitution Day
    "2026-12-31",  # New Year's Eve
    # 2027
    "2027-01-01",  # New Year's Day
    "2027-02-22",  # Substitution for Makha Bucha Day (Sun Feb 21)
    "2027-04-06",  # Chakri Memorial Day
    "2027-04-13",  # Songkran Festival
    "2027-04-14",  # Songkran Festival
    "2027-04-15",  # Songkran Festival
    "2027-05-03",  # Substitution for National Labour Day (Sat May 1)
    "2027-05-04",  # Coronation Day
    "2027-05-20",  # Visakha Bucha Day
    "2027-06-03",  # H.M. Queen Suthida's Birthday
    "2027-07-19",  # Substitution for Asahna Bucha Day (Sun Jul 18)
    "2027-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2027-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday / Mother's Day
    "2027-10-13",  # King Bhumibol Adulyadej Memorial Day
    "2027-10-25",  # Substitution for Chulalongkorn Day (Sat Oct 23)
    "2027-12-06",  # Substitution for King Bhumibol's Birthday / Father's Day (Sun Dec 5)
    "2027-12-10",  # Constitution Day
    "2027-12-31",  # New Year's Eve
}

# ==============================================================================
# Performance & High-Throughput Connection Optimization (60 req/sec Market Data)
# ==============================================================================
# 1. Update settrade_v2 default RateLimit to Settrade Market Data specs (60 req/sec)
#    and remove the restrictive 60 req/min order-placement throttle.
_orig_ratelimit_init = RateLimit.__init__
def _market_data_ratelimit_init(self, *args, **kwargs):
    _orig_ratelimit_init(self, *args, **kwargs)
    self.rate_per_second = 60
    self.remaining_per_second = 60
    self.rate_per_minute = 3600
    self.remaining_per_minute = 3600
RateLimit.__init__ = _market_data_ratelimit_init

# 2. Patch settrade_v2 with persistent requests.Session for HTTP Keep-Alive connection reuse
_http_session = requests.Session()
_http_adapter = requests.adapters.HTTPAdapter(
    pool_connections=25,
    pool_maxsize=25,
    max_retries=1
)
_http_session.mount("https://", _http_adapter)
_http_session.mount("http://", _http_adapter)

st_context.request = lambda method, url, **kwargs: _http_session.request(
    method=method,
    url=url,
    **kwargs
)


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
# 1. Trading Hours Guard
# ==============================================================================
def is_within_trading_hours(dt: datetime) -> Tuple[bool, str]:
    """
    Checks if current Asia/Bangkok time is within official TFEX trading sessions:
    - Monday to Friday only
    - Morning Session:   09:45 - 12:30
    - Afternoon Session: 14:15 - 16:55
    """
    weekday = dt.weekday()  # Monday is 0, Sunday is 6
    if weekday > 4:
        day_name = dt.strftime("%A")
        return False, f"Weekend ({day_name})"

    current_time = dt.time()
    morning_open = time(9, 45)
    morning_close = time(12, 30)
    afternoon_open = time(14, 15)
    afternoon_close = time(16, 55)

    if morning_open <= current_time <= morning_close:
        return True, "Morning Session (09:45 - 12:30)"
    if afternoon_open <= current_time <= afternoon_close:
        return True, "Afternoon Session (14:15 - 16:55)"

    return False, f"Outside sessions (Current time: {dt.strftime('%H:%M:%S')})"


# ==============================================================================
# 2. Dynamic TFEX Contract Resolution (Rollover Proof)
# ==============================================================================
def get_contract_last_trading_day(year: int, month: int) -> datetime:
    """
    Calculates TFEX contract last trading day:
    The business day immediately preceding the last business day of the contract month.
    Trading ceases at 16:30 hrs.
    """
    _, last_day = calendar.monthrange(year, month)
    biz_days = [
        date(year, month, d)
        for d in range(1, last_day + 1)
        if date(year, month, d).weekday() < 5
    ]
    if len(biz_days) >= 2:
        ltd = biz_days[-2]
    else:
        ltd = biz_days[-1]
    return BANGKOK_TZ.localize(datetime(ltd.year, ltd.month, ltd.day, 16, 30, 0))


def resolve_active_series_candidates(now_bkk: datetime) -> Tuple[List[str], str, str]:
    """
    Automatically discovers candidate active quarterly contract months (H, M, U, Z) without hardcoding.
    Both S50 Futures and SET50 Options strictly trade quarterly contracts.
    Returns:
    - candidate_futures_series: List of the 4 active quarterly futures series (e.g. ['Z26', 'H27', 'M27', 'U27'])
    - front_options_series: Nearest active quarterly options series (e.g. 'Z26')
    - next_options_series: Following active quarterly options series (e.g. 'H27')
    """
    active_quarterlies: List[str] = []
    for yr in range(now_bkk.year, now_bkk.year + 3):
        for qm in [3, 6, 9, 12]:
            q_ltd = get_contract_last_trading_day(yr, qm)
            if now_bkk <= q_ltd:
                code = MONTH_CODES[qm]
                yr_short = str(yr)[-2:]
                active_quarterlies.append(f"{code}{yr_short}")

    # Futures: The 4 actively traded quarterly contracts on TFEX (H, M, U, Z)
    candidate_futures_series = active_quarterlies[:4]

    # Options: Front Series and Next Series (Quarterly only)
    front_opt_series = active_quarterlies[0]
    next_opt_series = active_quarterlies[1]

    return candidate_futures_series, front_opt_series, next_opt_series


def generate_all_active_strikes(atm_base: Optional[float] = None) -> List[int]:
    """
    Generates all active strike prices covering the entire TFEX trading board.
    Uses 10-point increments, covering 750 to 1350 and ATM ± 250 points.
    """
    min_strike = 750
    max_strike = 1350
    if atm_base and atm_base > 0:
        atm_strike = round(atm_base / 10.0) * 10.0
        min_strike = min(min_strike, int(atm_strike - 250))
        max_strike = max(max_strike, int(atm_strike + 250))

    # Align to 10-point step
    min_strike = int(min_strike // 10 * 10)
    max_strike = int(max_strike // 10 * 10)
    return list(range(min_strike, max_strike + 10, 10))


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

    # Fallback to order book lists if present
    if bid is None and "bids" in raw and isinstance(raw["bids"], list) and raw["bids"]:
        bid = raw["bids"][0].get("price")
    if ask is None and "asks" in raw and isinstance(raw["asks"], list) and raw["asks"]:
        ask = raw["asks"][0].get("price")

    return {
        "bid": safe_float(bid),
        "ask": safe_float(ask),
        "last": safe_float(last),
        "volume": safe_float(vol) or 0.0,
        "open_interest": safe_float(oi) or 0.0
    }


# ==============================================================================
# 4. Turso Cloud SQLite HTTP Pipeline API (/v2/pipeline)
# ==============================================================================
def normalize_turso_pipeline_url(db_url: str) -> str:
    url = db_url.strip()
    if url.startswith("libsql://"):
        url = "https://" + url[9:]
    elif not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    url = url.rstrip("/")
    if not url.endswith("/v2/pipeline"):
        url = f"{url}/v2/pipeline"
    return url


def to_turso_arg(val: Any) -> Dict[str, Any]:
    if val is None:
        return {"type": "null"}
    if isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        return {"type": "float", "value": val}
    return {"type": "text", "value": str(val)}


def build_turso_pipeline_payload(
    spot_rows: List[Dict[str, Any]],
    futures_rows: List[Dict[str, Any]],
    options_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Constructs an atomic batch payload for Turso /v2/pipeline.
    Creates tables if not exists and inserts all collected rows.
    """
    requests_list: List[Dict[str, Any]] = []

    # 1. DDL Statements
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
    for ddl in [ddl_spot, ddl_futures, ddl_options, ddl_telegram_log]:
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": ddl.strip()}
        })

    # 2. Insert spot snapshot
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

    # 3. Insert futures snapshots
    insert_futures_sql = """
    INSERT INTO futures_snapshots (captured_at_bkk, symbol, bid, ask, last, volume, open_interest)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    for r in futures_rows:
        args = [
            to_turso_arg(r["captured_at_bkk"]),
            to_turso_arg(r["symbol"]),
            to_turso_arg(r["bid"]),
            to_turso_arg(r["ask"]),
            to_turso_arg(r["last"]),
            to_turso_arg(r["volume"]),
            to_turso_arg(r["open_interest"]),
        ]
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": insert_futures_sql.strip(), "args": args}
        })

    # 4. Insert options snapshots
    insert_options_sql = """
    INSERT INTO options_snapshots (
        captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in options_rows:
        args = [
            to_turso_arg(r["captured_at_bkk"]),
            to_turso_arg(r["symbol"]),
            to_turso_arg(r["series"]),
            to_turso_arg(r["strike"]),
            to_turso_arg(r["option_type"]),
            to_turso_arg(r["bid"]),
            to_turso_arg(r["ask"]),
            to_turso_arg(r["last"]),
            to_turso_arg(r["volume"]),
            to_turso_arg(r["open_interest"]),
        ]
        requests_list.append({
            "type": "execute",
            "stmt": {"sql": insert_options_sql.strip(), "args": args}
        })

    # Close pipeline transaction
    requests_list.append({"type": "close"})
    return {"requests": requests_list}


def execute_turso_pipeline(pipeline_url: str, auth_token: str, payload: Dict[str, Any]) -> None:
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    try:
        response = _http_session.post(pipeline_url, json=payload, headers=headers, timeout=30)
    except requests.exceptions.RequestException as e:
        logger.error(f"Turso network connection failed: {e}")
        sys.exit(1)

    if response.status_code != 200:
        # Sanitize error to prevent token leakage
        sanitized_msg = response.text.replace(auth_token, "***")
        logger.error(f"Turso API returned HTTP status {response.status_code}: {sanitized_msg}")
        sys.exit(1)

    data = response.json()
    results = data.get("results", [])
    errors = [
        item.get("error", {}).get("message", "Unknown statement error")
        for item in results
        if item.get("type") == "error"
    ]
    if errors:
        logger.error(f"Turso pipeline reported {len(errors)} SQL statement error(s): {errors[:3]}")
        sys.exit(1)

    logger.info("Turso HTTP pipeline batch write committed successfully.")


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
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    payload = {"requests": requests_list}
    response = _http_session.post(pipeline_url, json=payload, headers=headers, timeout=20)
    if response.status_code != 200:
        sanitized = response.text.replace(auth_token, "***")
        raise RuntimeError(f"Turso HTTP {response.status_code}: {sanitized}")
    data = response.json()
    return data.get("results", [])


def ensure_telegram_log_table(pipeline_url: str, auth_token: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS intraday_telegram_log (
        trade_date TEXT NOT NULL,
        checkpoint TEXT NOT NULL,
        notified_at_bkk TEXT NOT NULL,
        PRIMARY KEY (trade_date, checkpoint)
    );
    """
    reqs = [
        {"type": "execute", "stmt": {"sql": ddl.strip()}},
        {"type": "close"}
    ]
    try:
        query_turso_pipeline(pipeline_url, auth_token, reqs)
    except Exception as e:
        logger.warning(f"Could not verify intraday_telegram_log table: {e}")


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
    sql = f"SELECT COUNT(*) as cnt FROM spot_snapshots WHERE captured_at_bkk LIKE '{trade_date}%';"
    reqs = [
        {"type": "execute", "stmt": {"sql": sql}},
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
    sql_queries = [
        # 0. spot stats
        f"SELECT COUNT(*) as spot_count, MIN(captured_at_bkk) as min_ts, MAX(captured_at_bkk) as max_ts FROM spot_snapshots WHERE captured_at_bkk LIKE '{trade_date}%';",
        # 1. latest spot
        f"SELECT last, volume, value, captured_at_bkk FROM spot_snapshots WHERE captured_at_bkk LIKE '{trade_date}%' ORDER BY captured_at_bkk DESC LIMIT 1;",
        # 2. futures stats
        f"SELECT COUNT(*) as fut_count, COUNT(DISTINCT symbol) as fut_syms FROM futures_snapshots WHERE captured_at_bkk LIKE '{trade_date}%';",
        # 3. latest front futures
        f"SELECT symbol, last, bid, ask, volume, open_interest, captured_at_bkk FROM futures_snapshots WHERE captured_at_bkk LIKE '{trade_date}%' ORDER BY captured_at_bkk DESC, symbol ASC LIMIT 1;",
        # 4. options stats
        f"SELECT COUNT(*) as opt_count, COUNT(DISTINCT symbol) as opt_syms FROM options_snapshots WHERE captured_at_bkk LIKE '{trade_date}%';",
        # 5. morning bars
        f"SELECT COUNT(*) as morning_bars, MIN(captured_at_bkk) as min_morning, MAX(captured_at_bkk) as max_morning FROM spot_snapshots WHERE captured_at_bkk >= '{trade_date} 09:45:00' AND captured_at_bkk <= '{trade_date} 12:35:00';",
        # 6. afternoon bars
        f"SELECT COUNT(*) as afternoon_bars, MIN(captured_at_bkk) as min_afternoon, MAX(captured_at_bkk) as max_afternoon FROM spot_snapshots WHERE captured_at_bkk >= '{trade_date} 14:15:00' AND captured_at_bkk <= '{trade_date} 17:00:00';",
        # 7. pragma page_count
        "PRAGMA page_count;",
        # 8. pragma page_size
        "PRAGMA page_size;"
    ]

    reqs = [{"type": "execute", "stmt": {"sql": s}} for s in sql_queries]
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
    stats["fut_symbol"] = r3.get("symbol") or "S50"
    stats["fut_last"] = r3.get("last")
    stats["fut_bid"] = r3.get("bid")
    stats["fut_ask"] = r3.get("ask")
    stats["fut_oi"] = r3.get("open_interest") or 0.0

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

    # 7 & 8. Database size
    r7 = get_first_row(7)
    r8 = get_first_row(8)
    page_count = r7.get("page_count") or 0
    page_size = r8.get("page_size") or 4096
    db_bytes = page_count * page_size
    stats["db_pages"] = page_count
    if db_bytes >= 1024 * 1024:
        stats["db_size_str"] = f"{db_bytes / (1024 * 1024):.2f} MB"
    else:
        stats["db_size_str"] = f"{db_bytes / 1024:.1f} KB"

    return stats


def get_current_checkpoint(t: time) -> Optional[str]:
    # Checkpoint 1: 10:00 - 10:30 (Morning Open)
    if time(10, 0) <= t <= time(10, 30):
        return "MORNING_OPEN"
    # Checkpoint 2: 12:30 - 13:00 (Midday Break)
    if time(12, 30) <= t <= time(13, 0):
        return "MIDDAY_CLOSE"
    # Checkpoint 3: 14:30 - 15:00 (Afternoon Open)
    if time(14, 30) <= t <= time(15, 0):
        return "AFTERNOON_OPEN"
    # Checkpoint 4: 17:00 - 17:30 (Evening Close)
    if time(17, 0) <= t <= time(17, 30):
        return "EVENING_CLOSE"
    return None


def build_checkpoint_message(
    checkpoint: str,
    trade_date: str,
    now_bkk: datetime,
    stats: Dict[str, Any],
    is_manual: bool = False
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
            f"• <b>Storage:</b> Turso SQLite {db_size}"
        )
    elif checkpoint == "MIDDAY_CLOSE":
        m_pct = (morning_bars / 165.0) * 100.0 if morning_bars else 0.0
        health = "🟢 Complete" if morning_bars >= 160 else "⚠️ Missing Bars"
        body = (
            f"☕ <b>TFEX INTRADAY: MIDDAY BREAK REPORT</b>{mode_tag}\n\n"
            f"• <b>Status:</b> ⏸️ Morning Session Closed\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Morning Span:</b> <code>{min_ts}</code> – <code>{max_ts}</code>\n"
            f"• <b>Morning Bars:</b> <b>{morning_bars}</b> / 165 ({m_pct:.1f}%)\n"
            f"• <b>SET50 Spot:</b> <code>{spot_str}</code> | <b>{fut_sym}:</b> <code>{fut_str}</code>\n"
            f"• <b>Snapshots:</b> Spot: {spot_count} | Fut: {fut_count} | Opt: {opt_count}\n"
            f"• <b>Health Check:</b> {health}\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}"
        )
    elif checkpoint == "AFTERNOON_OPEN":
        warn = "\n⚠️ <b>Alert:</b> No bars captured yet for afternoon session!" if afternoon_bars == 0 else ""
        body = (
            f"📈 <b>TFEX INTRADAY: AFTERNOON OPEN STATUS</b>{mode_tag}\n\n"
            f"• <b>Status:</b> 🟢 Afternoon Session Active (14:15 - 16:55)\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Afternoon Bars (since 14:15):</b> <b>{afternoon_bars}</b> bars\n"
            f"• <b>Latest Timestamp:</b> <code>{max_ts}</code>\n"
            f"• <b>SET50 Spot:</b> <code>{spot_str}</code> | <b>{fut_sym}:</b> <code>{fut_str}</code>\n"
            f"• <b>Active Options:</b> <b>{opt_syms}</b> contracts\n"
            f"• <b>Storage:</b> Turso SQLite {db_size}{warn}"
        )
    else:  # EVENING_CLOSE or MANUAL_REPORT
        total_expected = 325.0
        c_pct = (spot_count / total_expected) * 100.0 if spot_count else 0.0
        if c_pct >= 98:
            verdict = "✅ Excellent (≥98%)"
        elif c_pct >= 90:
            verdict = "⚠️ Good (≥90%)"
        else:
            verdict = f"🚨 Incomplete ({c_pct:.1f}%)"

        title = "🏁 <b>TFEX INTRADAY: FULL-DAY CLOSE SUMMARY</b>" if checkpoint == "EVENING_CLOSE" else "📊 <b>TFEX INTRADAY: CURRENT STATUS REPORT</b>"
        body = (
            f"{title}{mode_tag}\n\n"
            f"• <b>Status:</b> 🔒 Market Closed\n"
            f"• <b>Trade Date:</b> <code>{trade_date}</code>\n"
            f"• <b>Trading Span:</b> <code>{min_ts}</code> – <code>{max_ts}</code>\n"
            f"• <b>Total Daily Bars:</b> <b>{spot_count}</b> / 325 ({c_pct:.1f}%)\n"
            f"  - Morning (09:45-12:30): {morning_bars} bars\n"
            f"  - Afternoon (14:15-16:55): {afternoon_bars} bars\n"
            f"• <b>Settlement Ref:</b> SET50 <code>{spot_str}</code> | {fut_sym} <code>{fut_str}</code>\n"
            f"• <b>Snapshots Saved:</b> Spot: {spot_count} | Fut: {fut_count} | Opt: {opt_count}\n"
            f"• <b>Database Storage:</b> <b>{db_size}</b> ({db_pages} pages)\n"
            f"• <b>Integrity Verdict:</b> {verdict}"
        )

    return f"{header}\n\n{body}"


def send_telegram_message(bot_token: str, chat_id: str, message: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        resp = _http_session.post(url, json=payload, timeout=10)
        if resp.status_code == 200 and resp.json().get("ok"):
            logger.info("Telegram alert sent successfully.")
            return True
        else:
            logger.error(f"Telegram API returned status {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Failed to deliver Telegram message: {e}")
        return False


def check_and_send_checkpoints(
    now_bkk: datetime,
    pipeline_url: str,
    turso_token: str,
    bot_token: str,
    chat_id: str,
    force_notify: bool = False
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
        msg = build_checkpoint_message(checkpoint, target_date, now_bkk, stats, is_manual=True)
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
        logger.info(f"[TELEGRAM] Checkpoint {checkpoint} for {today_str} already sent today. Skipping.")
        return False

    # 4. Market-Open Data Guard
    if not has_intraday_data_for_date(pipeline_url, turso_token, today_str):
        logger.info(f"[TELEGRAM] Market-Open Data Guard: No intraday data collected for {today_str}. Skipping {checkpoint} alert.")
        return False

    # 5. Query stats & send
    stats = query_turso_intraday_stats(pipeline_url, turso_token, today_str)
    msg = build_checkpoint_message(checkpoint, today_str, now_bkk, stats, is_manual=False)

    logger.info(f"[TELEGRAM] Triggering {checkpoint} alert for {today_str}...")
    success = send_telegram_message(bot_token, chat_id, msg)
    if success:
        now_ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")
        record_telegram_notification(pipeline_url, turso_token, today_str, checkpoint, now_ts_str)
        logger.info(f"[TELEGRAM] Recorded checkpoint {checkpoint} for {today_str} into intraday_telegram_log.")
    return success


# ==============================================================================
# 6. Worker Task for Option Quote Fetching
# ==============================================================================
def fetch_option_quote_task(
    market_data: Any,
    symbol: str,
    series: str,
    strike: float,
    opt_type: str,
    timestamp_bkk_str: str,
    limiter: Optional[PacedRateLimiter] = None
) -> Optional[Dict[str, Any]]:
    if limiter:
        limiter.acquire()

    try:
        quote = market_data.get_quote_symbol(symbol)
        if quote:
            fields = extract_derivatives_data(quote)
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
        # Strike not active / not listed on TFEX board
        return None
    except Exception as e:
        logger.debug(f"Option fetch error for {symbol}: {e}")
        return None
    return None


def load_dotenv_simple(env_path: str = ".env") -> None:
    """Loads environment variables from .env file if present (without external dependencies)."""
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


# ==============================================================================
# 6. Main Execution Workflow
# ==============================================================================
def main() -> None:
    start_time_perf = pytime.perf_counter()

    parser = argparse.ArgumentParser(description="TFEX 1-Minute Stateless Market Data Collector")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass trading hours guard and force execution immediately."
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Manually trigger and send the latest intraday status report to Telegram."
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

    # Automatically load local .env if present
    load_dotenv_simple(".env")

    now_bkk = datetime.now(BANGKOK_TZ)
    timestamp_bkk_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Collector initiated at {timestamp_bkk_str} (Asia/Bangkok)")

    # Read and Validate Environment Variables
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

    # Handle manual Telegram notification trigger (--notify)
    if args.notify:
        if not turso_url or not turso_token:
            logger.error("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN for --notify mode.")
            sys.exit(1)
        turso_pipeline_url = normalize_turso_pipeline_url(turso_url)
        logger.info("[NOTIFY] Executing manual Telegram status report...")
        success = check_and_send_checkpoints(
            now_bkk, turso_pipeline_url, turso_token,
            telegram_bot_token, telegram_chat_id, force_notify=True
        )
        sys.exit(0 if success else 1)

    # 1. Trading Hours & Calendar Holiday Guard
    if not args.force:
        # 1.1 Check Static Exchange Holidays (SET/TFEX Official Holidays)
        today_str = now_bkk.strftime("%Y-%m-%d")
        if today_str in SET_HOLIDAYS:
            logger.info(
                f"[GUARD] Today ({today_str}) is an official SET/TFEX exchange holiday. "
                f"Terminating gracefully (sys.exit(0)). Use --force to override."
            )
            sys.exit(0)

        # 1.2 Check Session Trading Hours
        within_hours, session_desc = is_within_trading_hours(now_bkk)
        if not within_hours:
            # Check if current time falls within off-market checkpoint windows (MIDDAY_CLOSE or EVENING_CLOSE)
            curr_chk = get_current_checkpoint(now_bkk.time())
            if curr_chk in ("MIDDAY_CLOSE", "EVENING_CLOSE") and turso_url and turso_token and telegram_bot_token:
                turso_pipeline_url = normalize_turso_pipeline_url(turso_url)
                logger.info(f"[CHECKPOINT] Current time is within {curr_chk} window. Running checkpoint alert evaluation...")
                check_and_send_checkpoints(
                    now_bkk, turso_pipeline_url, turso_token,
                    telegram_bot_token, telegram_chat_id, force_notify=False
                )

            logger.info(
                f"[GUARD] Current time {now_bkk.strftime('%A %H:%M:%S')} is outside TFEX trading sessions "
                f"({session_desc}). Terminating gracefully. Use --force to override."
            )
            sys.exit(0)
        logger.info(f"[GUARD] Trading hours verified: {session_desc}")
    else:
        logger.info("[GUARD] --force flag supplied: Bypassing trading hours and holiday guards.")

    # 2. Validate Settrade & Turso Credentials for Data Collection
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

    # 3. Initialize Settrade Open API Client
    try:
        if is_sandbox:
            config["environment"] = "uat"
            if not broker_id or broker_id.upper() == "SANDBOX":
                broker_id = Investor.SANDBOX

        logger.info(f"Connecting to Settrade Open API (broker: {broker_id}, sandbox: {is_sandbox})...")
        investor = Investor(
            app_id=app_id,
            app_secret=app_secret,
            broker_id=broker_id,
            app_code=app_code,
            is_auto_queue=False  # Direct high-throughput execution
        )
        market_data = investor.MarketData()
        logger.info("Settrade Open API authentication successful.")
    except Exception as e:
        logger.error(f"Failed to authenticate with Settrade Open API: {e}")
        sys.exit(1)

    # Resolve active series candidates (Rollover proof: Quarterly Options & Rolling Futures)
    futures_candidates, front_opt_series, next_opt_series = resolve_active_series_candidates(now_bkk)

    # 3.1 Ad-hoc / Emergency Holiday Guard (Check latest 1m candle of Front Futures after 09:46)
    if not args.force and now_bkk.time() >= time(9, 46):
        front_fut_symbol = f"S50{front_opt_series}"
        try:
            logger.info(f"[GUARD] Verifying live market activity on {front_fut_symbol} (time {now_bkk.strftime('%H:%M:%S')} >= 09:46)...")
            candles = market_data.get_candlestick(front_fut_symbol, interval="1m", limit=1)
            if candles and "time" in candles and candles["time"]:
                latest_candle_time = candles["time"][-1]
                candle_dt = datetime.fromtimestamp(latest_candle_time, BANGKOK_TZ)
                candle_date = candle_dt.date()
                today_date = now_bkk.date()
                if candle_date < today_date:
                    logger.info(
                        f"[GUARD] Ad-hoc / Emergency Holiday Detected: Latest 1m candle date for Front Futures {front_fut_symbol} "
                        f"is {candle_date} (< {today_date}). Market is closed today. Terminating gracefully (sys.exit(0))."
                    )
                    sys.exit(0)
                logger.info(
                    f"[GUARD] Live market verified: Latest 1m candle for {front_fut_symbol} is from today "
                    f"({candle_dt.strftime('%Y-%m-%d %H:%M')}). Market is ACTIVE."
                )
        except Exception as e:
            logger.warning(f"Could not verify market open status via {front_fut_symbol}: {e}")

    spot_snapshots: List[Dict[str, Any]] = []
    futures_snapshots: List[Dict[str, Any]] = []
    options_snapshots: List[Dict[str, Any]] = []

    # 4. Fetch SET50 Spot Index (with candlestick fallback for index symbols)
    spot_price_ref: Optional[float] = None
    try:
        try:
            spot_res = market_data.get_quote_symbol("SET50")
            spot_fields = extract_spot_data(spot_res)
        except Exception:
            # Index candlestick fallback for SET50 spot index
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

    # 5. Discover and Fetch Active S50 Futures
    logger.info(f"Evaluating candidate futures series: {futures_candidates}")

    front_futures_price: Optional[float] = None
    for series_tag in futures_candidates:
        symbol = f"S50{series_tag}"
        try:
            quote = market_data.get_quote_symbol(symbol)
            if quote:
                fields = extract_derivatives_data(quote)
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
            # Series not yet listed or already expired
            logger.debug(f"Candidate futures symbol {symbol} is not active in market.")
        except Exception as e:
            logger.warning(f"Error querying futures symbol {symbol}: {e}")

    # 6. Fetch SET50 Options for All Active Strikes (Front & Next Series)
    atm_base = spot_price_ref or front_futures_price or 900.0
    all_strikes = generate_all_active_strikes(atm_base)
    logger.info(
        f"Scanning SET50 Options across {len(all_strikes)} strikes "
        f"({all_strikes[0]} - {all_strikes[-1]}) for Front={front_opt_series} and Next={next_opt_series}..."
    )

    # Build full list of option targets to query
    option_targets: List[Tuple[str, str, float, str]] = []
    for opt_series in [front_opt_series, next_opt_series]:
        for strike in all_strikes:
            for opt_type in ["C", "P"]:
                symbol = f"S50{opt_series}{opt_type}{strike}"
                option_targets.append((symbol, opt_series, float(strike), opt_type))

    logger.info(f"Dispatching {len(option_targets)} Option queries (Workers={args.workers}, Pacing={args.max_rps:.0f} req/s)...")
    options_start = pytime.perf_counter()

    pacer = PacedRateLimiter(max_rps=args.max_rps)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures_tasks = [
            executor.submit(
                fetch_option_quote_task,
                market_data,
                sym,
                ser,
                stk,
                otp,
                timestamp_bkk_str,
                pacer
            )
            for (sym, ser, stk, otp) in option_targets
        ]

        for future in as_completed(futures_tasks):
            result = future.result()
            if result is not None:
                options_snapshots.append(result)

    options_duration = pytime.perf_counter() - options_start
    logger.info(
        f"Options scan finished in {options_duration:.2f}s: "
        f"Found {len(options_snapshots)} active contracts out of {len(option_targets)} scanned."
    )

    logger.info(
        f"Collection summary: Spot={len(spot_snapshots)}, "
        f"Futures={len(futures_snapshots)}, Options={len(options_snapshots)}"
    )

    total_records = len(spot_snapshots) + len(futures_snapshots) + len(options_snapshots)
    if total_records == 0:
        logger.warning("No records collected during this run. Skipping Turso write.")
        sys.exit(0)

    # 7. Write to Turso via HTTP Pipeline (Batch in single HTTP request)
    logger.info(f"Executing batch write to Turso Cloud SQLite ({total_records} rows)...")
    payload = build_turso_pipeline_payload(spot_snapshots, futures_snapshots, options_snapshots)
    execute_turso_pipeline(turso_pipeline_url, turso_token, payload)

    # 8. Check and trigger intraday Telegram checkpoints (MORNING_OPEN / AFTERNOON_OPEN)
    if telegram_bot_token and telegram_chat_id:
        check_and_send_checkpoints(
            now_bkk, turso_pipeline_url, turso_token,
            telegram_bot_token, telegram_chat_id, force_notify=False
        )

    total_elapsed = pytime.perf_counter() - start_time_perf
    logger.info(f"Collector job completed successfully in {total_elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
