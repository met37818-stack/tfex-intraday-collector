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
    Automatically discovers candidate active contract months without hardcoding.
    - Options: Strictly Quarterly Series (H, M, U, Z) only.
    - Futures: Nearest 3 consecutive months + next 3 quarterly months (H, M, U, Z).
    Returns:
    - candidate_futures_series: List of series suffixes (e.g. ['X26', 'Z26', 'F27', 'H27', 'M27', 'U27'])
    - front_options_series: Nearest active quarterly contract series (e.g. 'Z26')
    - next_options_series: Following active quarterly contract series (e.g. 'H27')
    """
    # 1. Options: Strictly Quarterly Series (H, M, U, Z)
    active_quarterlies: List[str] = []
    for yr in range(now_bkk.year, now_bkk.year + 3):
        for qm in [3, 6, 9, 12]:
            q_ltd = get_contract_last_trading_day(yr, qm)
            if now_bkk <= q_ltd:
                code = MONTH_CODES[qm]
                yr_short = str(yr)[-2:]
                active_quarterlies.append(f"{code}{yr_short}")

    front_opt_series = active_quarterlies[0]
    next_opt_series = active_quarterlies[1]

    # 2. Futures: Nearest 3 consecutive months + next 3 quarterly months
    y = now_bkk.year
    m = now_bkk.month

    # Check if current month's futures contract has already expired
    ltd = get_contract_last_trading_day(y, m)
    if now_bkk > ltd:
        m += 1
        if m > 12:
            m = 1
            y += 1

    # Generate sequence of upcoming months for the next 24 months
    upcoming_months: List[Tuple[int, int, str]] = []
    cy, cm = y, m
    for _ in range(24):
        code = MONTH_CODES[cm]
        yr_short = str(cy)[-2:]
        series_tag = f"{code}{yr_short}"
        upcoming_months.append((cy, cm, series_tag))
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1

    consecutive_3 = [item[2] for item in upcoming_months[:3]]
    quarterly_remaining = [
        item[2] for item in upcoming_months[3:]
        if item[1] in QUARTERLY_MONTHS
    ][:3]

    candidate_futures_series = consecutive_3 + quarterly_remaining
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
    for ddl in [ddl_spot, ddl_futures, ddl_options]:
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
# 5. Worker Task for Option Quote Fetching
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

    # 1. Trading Hours Guard
    if not args.force:
        within_hours, session_desc = is_within_trading_hours(now_bkk)
        if not within_hours:
            logger.info(
                f"[GUARD] Current time {now_bkk.strftime('%A %H:%M:%S')} is outside TFEX trading sessions "
                f"({session_desc}). Terminating gracefully. Use --force to override."
            )
            sys.exit(0)
        logger.info(f"[GUARD] Trading hours verified: {session_desc}")
    else:
        logger.info("[GUARD] --force flag supplied: Bypassing trading hours guard.")

    # 2. Read and Validate Environment Variables
    app_id = os.environ.get("SETTRADE_APP_ID", "").strip()
    app_secret = os.environ.get("SETTRADE_APP_SECRET", "").strip()
    broker_id = os.environ.get("SETTRADE_BROKER_ID", "").strip()
    app_code = os.environ.get("SETTRADE_APP_CODE", "ALGO").strip() or "ALGO"
    is_sandbox_raw = os.environ.get("SETTRADE_IS_SANDBOX", "false").strip().lower()
    is_sandbox = is_sandbox_raw in ("true", "1", "yes")

    turso_url = os.environ.get("TURSO_DATABASE_URL", "").strip()
    turso_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

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
    futures_candidates, front_opt_series, next_opt_series = resolve_active_series_candidates(now_bkk)
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

    total_elapsed = pytime.perf_counter() - start_time_perf
    logger.info(f"Collector job completed successfully in {total_elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
