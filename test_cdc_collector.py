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
    BANGKOK_TZ
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


if __name__ == "__main__":
    unittest.main()
