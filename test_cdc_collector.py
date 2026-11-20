"""
Unit tests for TFEX Intraday Collector CDC (Change Data Capture) Engine.
Tests:
1. is_tick_changed across 6 dimensions (bid, ask, bid_vol, ask_vol, last, volume)
2. Coalescing buffer behavior (merging multiple rapid ticks into single latest state per symbol)
3. Turso DDL schema migration generation (bid_vol, ask_vol)
4. Watchdog silence detection and health check
5. In-memory baseline snapshot generation (zero REST calls)
6. Checkpoint message building with CDC stats
"""
import unittest
from unittest.mock import MagicMock, patch
import time
from datetime import datetime
import pytz

from collector import (
    is_tick_changed,
    build_turso_pipeline_payload,
    SettradeClientManager,
    build_checkpoint_message,
    get_current_checkpoint,
    detect_session_window,
    is_within_trading_hours,
    SESSION_CONFIG,
    BANGKOK_TZ,
    get_penultimate_business_day,
    get_contract_last_trading_day,
    clean_mode_guard,
    _http_session,
    is_tick_within_session,
)


class TestCDCCollector(unittest.TestCase):

    def test_is_tick_changed_six_dimensions(self):
        base = {
            "bid": 920.0,
            "ask": 920.5,
            "bid_vol": 100.0,
            "ask_vol": 150.0,
            "last": 920.2,
            "volume": 5000.0
        }
        # Unchanged
        self.assertFalse(is_tick_changed(base, dict(base)))

        # None prev -> True
        self.assertTrue(is_tick_changed(None, base))

        # 1. Bid change
        c_bid = dict(base, bid=920.1)
        self.assertTrue(is_tick_changed(base, c_bid))

        # 2. Ask change
        c_ask = dict(base, ask=920.6)
        self.assertTrue(is_tick_changed(base, c_ask))

        # 3. Bid volume change
        c_bvol = dict(base, bid_vol=120.0)
        self.assertTrue(is_tick_changed(base, c_bvol))

        # 4. Ask volume change
        c_avol = dict(base, ask_vol=180.0)
        self.assertTrue(is_tick_changed(base, c_avol))

        # 5. Last price change
        c_last = dict(base, last=920.5)
        self.assertTrue(is_tick_changed(base, c_last))

        # 6. Cumulative volume change
        c_vol = dict(base, volume=5010.0)
        self.assertTrue(is_tick_changed(base, c_vol))

    def test_coalescing_buffer_deduplication(self):
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)
        
        with patch("collector.datetime") as mock_dt:
            mock_now = datetime(2026, 11, 20, 10, 15, 0, tzinfo=BANGKOK_TZ)
            mock_dt.now.return_value = mock_now
            mock_dt.strftime = datetime.strftime

            # Simulate 10 incoming ticks for symbol S50Z26 within 1 micro-batch window
            for i in range(10):
                mock_msg = {
                    "is_success": True,
                    "data": {
                        "symbol": "S50Z26",
                        "bid_price1": 920.0 + i * 0.1,
                        "ask_price1": 920.5 + i * 0.1,
                        "bid_volume1": 100 + i,
                        "ask_volume1": 200 + i,
                        "last": 920.2 + i * 0.1,
                        "total_volume": 1000 + i
                    }
                }
                mgr.on_bid_offer(mock_msg)

            # Buffer should contain exactly 1 entry for S50Z26 (the coalesced latest state)
            self.assertEqual(len(mgr.coalesced_buffer), 1)
            latest = mgr.coalesced_buffer["S50Z26"]
            self.assertAlmostEqual(latest["bid"], 920.9)
            self.assertEqual(latest["bid_vol"], 109)
            self.assertEqual(mgr.cdc_stats["received"], 10)
            self.assertEqual(mgr.cdc_stats["changed"], 10)
            self.assertEqual(mgr.cdc_stats["deduped"], 0)

            # Send an identical 11th tick -> should be filtered out by noise filter
            identical_msg = {
                "is_success": True,
                "data": {
                    "symbol": "S50Z26",
                    "bid_price1": 920.9,
                    "ask_price1": 921.4,
                    "bid_volume1": 109,
                    "ask_volume1": 209,
                    "last": 921.1,
                    "total_volume": 1009
                }
            }
            mgr.on_bid_offer(identical_msg)
            self.assertEqual(mgr.cdc_stats["received"], 11)
            self.assertEqual(mgr.cdc_stats["changed"], 10)
            self.assertEqual(mgr.cdc_stats["deduped"], 1)

    def test_watchdog_silence_detection(self):
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)
        mgr.last_tick_time = time.time() - 25.0  # 25 seconds ago
        
        is_healthy, elapsed = mgr.check_watchdog_health()
        self.assertFalse(is_healthy)
        self.assertGreaterEqual(elapsed, 25.0)

        # Reset tick time
        mgr.last_tick_time = time.time()
        is_healthy, elapsed = mgr.check_watchdog_health()
        self.assertTrue(is_healthy)
        self.assertLess(elapsed, 5.0)

    @patch("collector.save_cdc_batch_to_turso")
    def test_in_memory_baseline_snapshot(self, mock_save):
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)
        # Prepopulate cache
        mgr.latest_market_state["S50Z26"] = {
            "symbol": "S50Z26",
            "bid": 920.0,
            "ask": 920.5,
            "bid_vol": 100,
            "ask_vol": 100,
            "last": 920.0,
            "volume": 1000
        }
        mgr.latest_market_state["S50Z26C920"] = {
            "symbol": "S50Z26C920",
            "bid": 15.0,
            "ask": 15.5,
            "bid_vol": 50,
            "ask_vol": 50,
            "last": 15.2,
            "volume": 200
        }

        fut_c, opt_c = mgr.capture_baseline_snapshot("https://dummy", "token", "09:45_MORNING_OPEN")
        self.assertEqual(fut_c, 1)
        self.assertEqual(opt_c, 1)
        self.assertTrue(mock_save.called)

    def test_checkpoint_message_with_cdc_stats(self):
        now_bkk = datetime.now(BANGKOK_TZ)
        stats = {
            "spot_count": 150,
            "spot_last": 925.3,
            "spot_vol": 1000000.0,
            "fut_count": 150,
            "fut_symbol": "S50Z26",
            "fut_last": 926.0,
            "fut_oi": 500000.0,
            "opt_count": 3000,
            "opt_syms": 120,
            "morning_bars": 150,
            "afternoon_bars": 0,
            "min_ts": "09:45:00",
            "max_ts": "12:15:00",
            "db_size_str": "12.4 MB (0.24% ของโควต้า 5 GB)",
            "db_pages": 3100,
            "error_count": 0
        }
        cdc_stats = {
            "received": 50000,
            "changed": 12000,
            "deduped": 38000,
            "flushes": 400,
            "rows_written": 12000
        }
        msg = build_checkpoint_message(
            checkpoint="MIDDAY_CLOSE",
            trade_date=now_bkk.strftime("%Y-%m-%d"),
            now_bkk=now_bkk,
            stats=stats,
            is_manual=False,
            cdc_stats=cdc_stats
        )
        self.assertIn("CDC Stream Stats:", msg)
        self.assertIn("50,000", msg)
        self.assertIn("12,000", msg)
        self.assertIn("38,000", msg)
        self.assertIn("76.0% reduction", msg)

    def test_clear_session_cache(self):
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)
        mgr.latest_market_state["S50Z26"] = {"symbol": "S50Z26", "bid": 920.0}
        mgr.last_persisted_state["S50Z26"] = {"symbol": "S50Z26", "bid": 920.0}
        mgr.coalesced_buffer["S50Z26"] = {"symbol": "S50Z26", "bid": 920.0}
        mgr.bid_offer_cache["S50Z26"] = {"symbol": "S50Z26"}

        mgr.clear_session_cache()
        self.assertEqual(len(mgr.latest_market_state), 0)
        self.assertEqual(len(mgr.last_persisted_state), 0)
        self.assertEqual(len(mgr.coalesced_buffer), 0)
        self.assertEqual(len(mgr.bid_offer_cache), 0)

    @patch("collector.save_cdc_batch_to_turso")
    def test_open_interest_passes_through_baseline(self, mock_save):
        # The session loop refreshes the front future's OI from a REST quote, and
        # every writer used to hardcode None, so the column was always NULL.
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)
        mgr.latest_market_state["S50Z26"] = {
            "symbol": "S50Z26",
            "bid": 920.0, "ask": 920.5, "bid_vol": 10.0, "ask_vol": 20.0,
            "last": 920.2, "volume": 100.0, "open_interest": 123456.0,
            "captured_at_bkk": "2026-11-19 09:45:00"
        }
        mgr.latest_market_state["S50Z26C900"] = {
            "symbol": "S50Z26C900",
            "bid": 22.0, "ask": 22.5, "bid_vol": 5.0, "ask_vol": 10.0,
            "last": 22.2, "volume": 50.0, "captured_at_bkk": "2026-11-19 09:45:00"
        }
        mgr.capture_baseline_snapshot("https://dummy", "token", "09:45_MORNING_OPEN")
        self.assertTrue(mock_save.called)
        _, _, fut_rows, opt_rows = mock_save.call_args[0][:4]
        self.assertEqual(fut_rows[0]["open_interest"], 123456.0, "known OI must be persisted")
        self.assertIsNone(opt_rows[0]["open_interest"], "no OI in the stream stays NULL")

    def test_health_check_complete_and_partial(self):
        now_bkk = datetime.now(BANGKOK_TZ)
        base_stats = {
            "spot_count": 165,
            "spot_last": 925.3,
            "spot_vol": 1000000.0,
            "fut_count": 165,
            "fut_symbol": "S50Z26",
            "fut_last": 926.0,
            "fut_oi": 500000.0,
            "opt_count": 3000,
            "opt_syms": 120,
            "min_ts": "09:45:00",
            "max_ts": "12:30:00",
            "db_size_str": "12.4 MB (0.24% ของโควต้า 5 GB)",
            "db_pages": 3100,
            "error_count": 0
        }

        # 1. 100% (165/165 bars) -> 🟢 Complete
        stats_100 = dict(base_stats, morning_bars=165)
        msg_100 = build_checkpoint_message("MIDDAY_CLOSE", "2026-11-20", now_bkk, stats_100)
        self.assertIn("Health Check:</b> 🟢 Complete", msg_100)

        # 2. 99.4% (164/165 bars) -> 🟢 Complete (>= 99%)
        stats_99 = dict(base_stats, morning_bars=164)
        msg_99 = build_checkpoint_message("MIDDAY_CLOSE", "2026-11-20", now_bkk, stats_99)
        self.assertIn("Health Check:</b> 🟢 Complete", msg_99)

        # 3. 98.8% (163/165 bars) -> 🟡 Partial (163/165 bars) (< 99%)
        stats_98 = dict(base_stats, morning_bars=163)
        msg_98 = build_checkpoint_message("MIDDAY_CLOSE", "2026-11-20", now_bkk, stats_98)
        self.assertIn("Health Check:</b> 🟡 Partial (163/165 bars)", msg_98)

        # 4. 90.9% (150/165 bars) -> 🟡 Partial (150/165 bars)
        stats_90 = dict(base_stats, morning_bars=150)
        msg_90 = build_checkpoint_message("MIDDAY_CLOSE", "2026-11-20", now_bkk, stats_90)
        self.assertIn("Health Check:</b> 🟡 Partial (150/165 bars)", msg_90)

    def test_on_bid_offer_preserves_last_and_volume(self):
        mgr = SettradeClientManager("app_id", "secret", "broker", "code", False)

        # First tick has trade execution price (last=920.5, volume=50)
        t1 = {
            "is_success": True,
            "data": {
                "symbol": "S50Z26",
                "bid_price1": 920.4,
                "ask_price1": 920.6,
                "bid_volume1": 10,
                "ask_volume1": 20,
                "last": 920.5,
                "total_volume": 50
            }
        }
        with patch("collector.datetime") as mock_dt:
            # Within market hours: 10:15:00
            mock_now = datetime(2026, 11, 20, 10, 15, 0, tzinfo=BANGKOK_TZ)
            mock_dt.now.return_value = mock_now
            mock_dt.strftime = datetime.strftime
            mgr.on_bid_offer(t1)

            self.assertEqual(mgr.latest_market_state["S50Z26"]["last"], 920.5)
            self.assertEqual(mgr.latest_market_state["S50Z26"]["volume"], 50)

            # Second tick is a pure bid/offer order book change without last price (last=None, volume=0)
            t2 = {
                "is_success": True,
                "data": {
                    "symbol": "S50Z26",
                    "bid_price1": 920.3,
                    "ask_price1": 920.5,
                    "bid_volume1": 15,
                    "ask_volume1": 25,
                    "last": None,
                    "total_volume": None
                }
            }
            mgr.on_bid_offer(t2)

            # Verify that last and volume are PRESERVED and not overwritten by None / 0
            self.assertEqual(mgr.latest_market_state["S50Z26"]["last"], 920.5)
            self.assertEqual(mgr.latest_market_state["S50Z26"]["volume"], 50)
            self.assertEqual(mgr.latest_market_state["S50Z26"]["bid"], 920.3)
            self.assertEqual(mgr.latest_market_state["S50Z26"]["ask"], 920.5)

    def test_session_config_and_cutoff_timings(self):
        from datetime import time

        # 1. Pre-warm timings
        self.assertEqual(SESSION_CONFIG["MORNING"]["pre_warm_time"], time(9, 44, 30))
        self.assertEqual(SESSION_CONFIG["AFTERNOON"]["pre_warm_time"], time(13, 44, 30))

        # 2. Market open timings
        self.assertEqual(SESSION_CONFIG["MORNING"]["market_open"], time(9, 45))
        self.assertEqual(SESSION_CONFIG["AFTERNOON"]["market_open"], time(13, 45))

        # 3. Market close and cutoff timings
        self.assertEqual(SESSION_CONFIG["MORNING"]["market_close"], time(12, 30))
        self.assertEqual(SESSION_CONFIG["MORNING"]["cutoff_time"], time(12, 30, 5))
        self.assertEqual(SESSION_CONFIG["AFTERNOON"]["market_close"], time(16, 55))
        self.assertEqual(SESSION_CONFIG["AFTERNOON"]["cutoff_time"], time(16, 55, 5))

        # 4. Checkpoint mapping
        self.assertEqual(get_current_checkpoint(time(10, 0)), "MORNING_OPEN")
        self.assertEqual(get_current_checkpoint(time(12, 29, 59)), "MORNING_OPEN")
        self.assertEqual(get_current_checkpoint(time(12, 30, 0)), "MIDDAY_CLOSE")
        self.assertEqual(get_current_checkpoint(time(13, 0)), "MIDDAY_CLOSE")
        self.assertEqual(get_current_checkpoint(time(13, 44, 59)), "MIDDAY_CLOSE")
        self.assertEqual(get_current_checkpoint(time(14, 0)), "AFTERNOON_OPEN")
        self.assertEqual(get_current_checkpoint(time(16, 54, 59)), "AFTERNOON_OPEN")
        self.assertEqual(get_current_checkpoint(time(16, 55, 0)), "EVENING_CLOSE")
        self.assertEqual(get_current_checkpoint(time(17, 30)), "EVENING_CLOSE")

        # 5. Trading hours check at cutoff boundaries
        fri_12_30_00 = datetime(2026, 11, 20, 12, 30, 0, tzinfo=BANGKOK_TZ)
        fri_12_30_05 = datetime(2026, 11, 20, 12, 30, 5, tzinfo=BANGKOK_TZ)
        fri_12_30_06 = datetime(2026, 11, 20, 12, 30, 6, tzinfo=BANGKOK_TZ)

        in_h, _ = is_within_trading_hours(fri_12_30_00)
        self.assertTrue(in_h)
        in_h, _ = is_within_trading_hours(fri_12_30_05)
        self.assertTrue(in_h)
        in_h, _ = is_within_trading_hours(fri_12_30_06)
        self.assertFalse(in_h)

        # Afternoon trading hours boundaries (13:45:00 - 16:55:05)
        fri_13_44_59 = datetime(2026, 11, 20, 13, 44, 59, tzinfo=BANGKOK_TZ)
        fri_13_45_00 = datetime(2026, 11, 20, 13, 45, 0, tzinfo=BANGKOK_TZ)
        fri_16_55_05 = datetime(2026, 11, 20, 16, 55, 5, tzinfo=BANGKOK_TZ)
        fri_16_55_06 = datetime(2026, 11, 20, 16, 55, 6, tzinfo=BANGKOK_TZ)

        in_h, _ = is_within_trading_hours(fri_13_44_59)
        self.assertFalse(in_h)
        in_h, _ = is_within_trading_hours(fri_13_45_00)
        self.assertTrue(in_h)
        in_h, _ = is_within_trading_hours(fri_16_55_05)
        self.assertTrue(in_h)
        in_h, _ = is_within_trading_hours(fri_16_55_06)
        self.assertFalse(in_h)

    def test_full_day_355_bars_health_verdict(self):
        now_bkk = datetime.now(BANGKOK_TZ)
        base_stats = {
            "spot_last": 925.3,
            "spot_vol": 1000000.0,
            "fut_count": 355,
            "fut_symbol": "S50Z26",
            "fut_last": 926.0,
            "fut_oi": 500000.0,
            "opt_count": 5000,
            "opt_syms": 120,
            "morning_bars": 165,
            "afternoon_bars": 190,
            "min_ts": "09:45:00",
            "max_ts": "16:55:00",
            "db_size_str": "15.0 MB",
            "db_pages": 3800,
            "error_count": 0
        }

        # 1. 355/355 bars (100%) -> 🟢 Complete (>= 99%)
        stats_355 = dict(base_stats, spot_count=355)
        msg_355 = build_checkpoint_message("EVENING_CLOSE", "2026-11-20", now_bkk, stats_355)
        self.assertIn("Integrity Verdict:</b> 🟢 Complete (≥99%)", msg_355)
        self.assertIn("<b>355</b> / 355 (100.0%)", msg_355)

        # 2. 352/355 bars (99.2%) -> 🟢 Complete (>= 99%)
        stats_352 = dict(base_stats, spot_count=352)
        msg_352 = build_checkpoint_message("EVENING_CLOSE", "2026-11-20", now_bkk, stats_352)
        self.assertIn("Integrity Verdict:</b> 🟢 Complete (≥99%)", msg_352)

        # 3. 320/355 bars (90.1%) -> 🟡 Partial (320/355 bars)
        stats_320 = dict(base_stats, spot_count=320)
        msg_320 = build_checkpoint_message("EVENING_CLOSE", "2026-11-20", now_bkk, stats_320)
        self.assertIn("Integrity Verdict:</b> 🟡 Partial (320/355 bars)", msg_320)

        # 4. 300/355 bars (84.5%) -> 🚨 Incomplete (300/355 bars)
        stats_300 = dict(base_stats, spot_count=300)
        msg_300 = build_checkpoint_message("EVENING_CLOSE", "2026-11-20", now_bkk, stats_300)
        self.assertIn("Integrity Verdict:</b> 🚨 Incomplete (300/355 bars)", msg_300)

    def test_get_penultimate_business_day_with_holidays(self):
        # In Dec 2026: Dec 31 is holiday. Business days end on Dec 30 (last) and Dec 29 (penultimate).
        p_day = get_penultimate_business_day(2026, 12)
        self.assertEqual(p_day.strftime("%Y-%m-%d"), "2026-12-29")

        ltd = get_contract_last_trading_day(2026, 12)
        self.assertEqual(ltd.strftime("%Y-%m-%d %H:%M:%S"), "2026-12-29 16:30:00")


class TestCleanModeGuard(unittest.TestCase):
    """--clean wipes three tables with no WHERE clause; it needs a second key."""

    def test_clean_requires_explicit_confirmation(self):
        self.assertIsNone(clean_mode_guard(False, False), "non-clean runs are never blocked")
        self.assertIsNone(clean_mode_guard(False, True))
        refusal = clean_mode_guard(True, False)
        self.assertIsNotNone(refusal)
        self.assertIn("--confirm-clean", refusal)
        self.assertIn("EVERY row", refusal)
        self.assertIsNone(clean_mode_guard(True, True))


if __name__ == "__main__":
    unittest.main()


class TestHttpSessionPerThread(unittest.TestCase):
    """Option quotes are fetched from N worker threads; Session is not thread-safe."""

    def test_session_is_per_thread(self):
        import threading

        main_session = _http_session()
        self.assertIs(main_session, _http_session(), "same thread must reuse its session")

        seen = {}

        def grab():
            seen["worker"] = _http_session()

        thread = threading.Thread(target=grab)
        thread.start()
        thread.join()

        self.assertIsNot(main_session, seen["worker"], "worker thread must get its own session")


class TestSessionTickGuard(unittest.TestCase):
    """Ticks outside the two continuous sessions must not be recorded."""

    def test_pre_open_ticks_are_rejected(self):
        from datetime import time as t

        self.assertFalse(is_tick_within_session(t(8, 59)))
        self.assertFalse(is_tick_within_session(t(9, 0)))
        self.assertFalse(is_tick_within_session(t(9, 44, 59)))
        self.assertTrue(is_tick_within_session(t(9, 45)))

    def test_session_boundaries(self):
        from datetime import time as t

        self.assertTrue(is_tick_within_session(t(12, 30, 5)))
        self.assertFalse(is_tick_within_session(t(12, 30, 6)))
        self.assertFalse(is_tick_within_session(t(13, 44, 59)))
        self.assertTrue(is_tick_within_session(t(13, 45)))
        self.assertTrue(is_tick_within_session(t(16, 55, 5)))
        self.assertFalse(is_tick_within_session(t(16, 55, 6)))


class TestSqlParameterisation(unittest.TestCase):
    """Dates must be bound, never interpolated into the SQL text."""

    DATE = "2026-11-20"

    @patch("collector.query_turso_pipeline")
    def test_stat_queries_bind_every_date(self, mock_query):
        from collector import query_turso_intraday_stats

        mock_query.return_value = []
        query_turso_intraday_stats("https://dummy/v2/pipeline", "tok", self.DATE)

        stmts = [r["stmt"] for r in mock_query.call_args[0][2] if r["type"] == "execute"]
        self.assertTrue(stmts)
        for stmt in stmts:
            self.assertNotIn(self.DATE, stmt["sql"], f"date interpolated into SQL: {stmt['sql'][:60]}")
            if stmt["sql"].startswith("PRAGMA"):
                continue
            self.assertIn("?", stmt["sql"])
            self.assertTrue(stmt.get("args"), f"no bound args for: {stmt['sql'][:60]}")

    @patch("collector.query_turso_pipeline")
    def test_intraday_data_check_binds_date(self, mock_query):
        from collector import has_intraday_data_for_date

        mock_query.return_value = []
        has_intraday_data_for_date("https://dummy/v2/pipeline", "tok", self.DATE)

        stmt = mock_query.call_args[0][2][0]["stmt"]
        self.assertNotIn(self.DATE, stmt["sql"])
        self.assertEqual(stmt["args"][0], {"type": "text", "value": f"{self.DATE}%"})

    @patch("collector.query_turso_pipeline")
    def test_worker_active_check_binds_date(self, mock_query):
        from collector import is_worker_already_active

        mock_query.return_value = []
        is_worker_already_active("https://dummy/v2/pipeline", "tok", datetime(2026, 11, 20, 10, 0, tzinfo=BANGKOK_TZ))

        stmt = mock_query.call_args[0][2][0]["stmt"]
        self.assertNotIn(self.DATE, stmt["sql"])
        self.assertEqual(stmt["args"][0], {"type": "text", "value": f"{self.DATE}%"})


class TestTelegramTokenHygiene(unittest.TestCase):
    """The bot token lives in the request URL; it must never reach a log line."""

    def test_transport_error_does_not_log_the_token(self):
        import requests
        from collector import send_telegram_message

        token = "123456789:AAFakeIntradayTokenMustNotLeak"
        boom = requests.ConnectionError(
            f"HTTPSConnectionPool(host='api.telegram.org'): Max retries exceeded with url: /bot{token}/sendMessage"
        )

        class FakeSession:
            def post(self, *a, **kw):
                raise boom

        with patch("collector._http_session", return_value=FakeSession()):
            with self.assertLogs("tfex_collector", level="ERROR") as logs:
                ok = send_telegram_message(token, "1", "hello")

        self.assertFalse(ok)
        joined = " ".join(logs.output)
        self.assertNotIn(token, joined, "bot token leaked into logs")
        self.assertIn("ConnectionError", joined)


class TestMarketInactiveExitCode(unittest.TestCase):
    """An inactive market must not exit 0 (cron would call it a success)."""

    def test_no_candles_today_exits_nonzero(self):
        from datetime import date, timedelta
        from collector import check_holiday_early_exit

        yesterday = datetime.now(BANGKOK_TZ).date() - timedelta(days=1)
        stale_epoch = int(datetime(yesterday.year, yesterday.month, yesterday.day, 15, 0, tzinfo=BANGKOK_TZ).timestamp())

        class FakeMarketData:
            def get_candlestick(self, symbol, interval="1m", limit=1):
                return {"time": [stale_epoch], "close": [900.0]}

        with self.assertRaises(SystemExit) as ctx:
            check_holiday_early_exit(FakeMarketData(), "Z26", datetime.now(BANGKOK_TZ))
        self.assertEqual(ctx.exception.code, 3)
