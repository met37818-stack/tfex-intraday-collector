"""
Lifecycle wiring tests for the intraday collector's entry point.

The collector used to leave no trace in Telegram when a session started, ended
normally, or died - including the exit 3 path that exists to flag a data outage.
These tests drive main() with the broker and database stubbed out and assert the
three distinct messages.
"""
import sys
import unittest
from unittest import mock

import collector
from tfex_common import lifecycle

ENV = {
    "SETTRADE_APP_ID": "app",
    "SETTRADE_APP_SECRET": "secret",
    "SETTRADE_BROKER_ID": "broker",
    "SETTRADE_APP_CODE": "ALGO",
    "SETTRADE_IS_SANDBOX": "false",
    "TURSO_DATABASE_URL": "libsql://example.turso.io",
    "TURSO_AUTH_TOKEN": "token",
    "TELEGRAM_BOT_TOKEN": "123:abc",
    "TELEGRAM_CHAT_ID": "42",
}


class _Capture:
    def __init__(self):
        self.calls = []

    def __call__(self, token, chat, text):
        self.calls.append(text)
        return True

    def kinds(self):
        return [("started" if "เริ่มทำงาน" in t else
                 "finished" if "จบงาน" in t else
                 "failed" if "ล้มเหลว" in t else "?") for t in self.calls]


class TestMainLifecycle(unittest.TestCase):
    def setUp(self):
        self.cap = _Capture()
        self._orig_send = lifecycle._send
        lifecycle._send = self.cap
        self.patches = [
            mock.patch.dict("os.environ", ENV, clear=False),
            mock.patch.object(collector, "load_dotenv_simple", lambda *a, **k: None),
            mock.patch.object(collector, "SettradeClientManager", lambda **kw: object()),
            mock.patch.object(collector, "normalize_turso_pipeline_url", lambda u: "https://db/v2/pipeline"),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        lifecycle._send = self._orig_send
        for p in self.patches:
            p.stop()

    def _run_main(self, argv, worker_side_effect=None):
        with mock.patch.object(sys, "argv", ["collector.py"] + argv):
            if worker_side_effect is not None:
                with mock.patch.object(collector, "run_session_worker", side_effect=worker_side_effect):
                    return collector.main()
            with mock.patch.object(collector, "run_session_worker", return_value=None):
                return collector.main()

    def test_session_clean_run_sends_start_and_finish(self):
        self._run_main(["--session"])
        self.assertEqual(self.cap.kinds(), ["started", "finished"])
        self.assertIn("tfex-intraday-collector", self.cap.calls[0])
        self.assertIn("session", self.cap.calls[0])
        self.assertIn("session loop ended", self.cap.calls[1])

    def test_market_inactive_exit_three_is_reported_as_failure(self):
        with self.assertRaises(SystemExit):
            self._run_main(["--session"], worker_side_effect=SystemExit(3))
        self.assertEqual(self.cap.kinds(), ["started", "failed"])
        self.assertIn("exit 3", self.cap.calls[1])

    def test_unhandled_exception_is_reported_and_propagates(self):
        with self.assertRaises(ConnectionError):
            self._run_main(["--session"], worker_side_effect=ConnectionError("sdk dropped"))
        self.assertEqual(self.cap.kinds(), ["started", "failed"])
        self.assertIn("ConnectionError", self.cap.calls[1])

    def test_once_mode_finishes_with_its_own_label(self):
        with mock.patch.object(sys, "argv", ["collector.py", "--once"]):
            with mock.patch.object(collector, "run_single_snapshot", return_value=None):
                collector.main()
        self.assertEqual(self.cap.kinds(), ["started", "finished"])
        self.assertIn("single snapshot", self.cap.calls[1])

    def test_missing_telegram_credentials_sends_nothing(self):
        env = {k: v for k, v in ENV.items() if not k.startswith("TELEGRAM")}
        with mock.patch.dict("os.environ", env, clear=False):
            for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
                os_env = mock.patch.dict("os.environ", {key: ""}, clear=False)
                os_env.start()
                self.addCleanup(os_env.stop)
            self._run_main(["--session"])
        self.assertEqual(self.cap.calls, [])


if __name__ == "__main__":
    unittest.main()
