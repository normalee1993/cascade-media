"""v1.11.0 webhook-server security hardening (findings S1-S5, S40).

Exercises the real WebhookHandler on a loopback BoundedThreadingHTTPServer, so
auth, Content-Type, input validation, queue bounds, the connection cap and the
whole-connection deadline are tested end to end over real sockets. The webhook
executor is mocked, so no child process is ever started.
"""

import base64
import http.client
import json
import os
import socket
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scheduler  # noqa: E402
import security_utils  # noqa: E402

SECRET = "correct-horse-battery-staple"


class _LiveServer(unittest.TestCase):
    max_connections = 8

    def setUp(self):
        patches = [
            patch.object(scheduler, "WEBHOOK_SECRET", SECRET),
            patch.object(scheduler, "webhook_executor", MagicMock()),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        with scheduler._pending_lock:
            scheduler._pending_series.clear()
        self.addCleanup(lambda: scheduler._pending_series.clear())
        self.server = scheduler.BoundedThreadingHTTPServer(
            ("127.0.0.1", 0), scheduler.WebhookHandler, max_connections=self.max_connections)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)

    def _stop(self):
        self.server.shutdown()
        self.server.server_close()

    def post(self, payload, path="/", headers=None, content_type="application/json"):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        hdrs = {"Content-Length": str(len(body))}
        if content_type:
            hdrs["Content-Type"] = content_type
        hdrs.update(headers or {})
        conn.request("POST", path, body=body, headers=hdrs)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, (json.loads(data) if data else None)

    @staticmethod
    def basic(password, user="sonarr"):
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}


class WebhookAuthTests(_LiveServer):
    ADD = {"eventType": "SeriesAdd", "series": {"id": 42, "title": "Show"}}

    def test_no_secret_configured_refuses_with_403(self):
        with patch.object(scheduler, "WEBHOOK_SECRET", ""):
            status, _ = self.post(self.ADD, path="/?token=anything")
        self.assertEqual(status, 403)
        scheduler.webhook_executor.submit.assert_not_called()

    def test_missing_secret_is_401(self):
        status, _ = self.post(self.ADD)
        self.assertEqual(status, 401)
        scheduler.webhook_executor.submit.assert_not_called()

    def test_wrong_secret_is_401(self):
        status, _ = self.post(self.ADD, headers=self.basic("wrong"))
        self.assertEqual(status, 401)
        status, _ = self.post(self.ADD, path="/?token=wrong")
        self.assertEqual(status, 401)

    def test_basic_auth_password_accepted(self):
        status, data = self.post(self.ADD, headers=self.basic(SECRET))
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "triggered")
        scheduler.webhook_executor.submit.assert_called_once_with(
            scheduler.process_webhook_series, 42)

    def test_query_token_accepted(self):
        status, data = self.post(self.ADD, path=f"/?token={SECRET}")
        self.assertEqual(status, 200)

    def test_text_plain_cross_site_post_rejected(self):
        """A browser 'simple request' (form enctype=text/plain) can't reach the
        handler logic even with the right token in the URL."""
        status, _ = self.post(self.ADD, path=f"/?token={SECRET}", content_type="text/plain")
        self.assertEqual(status, 415)
        scheduler.webhook_executor.submit.assert_not_called()

    def test_health_get_needs_no_auth(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("GET", "/")
        self.assertEqual(conn.getresponse().status, 200)
        conn.close()


class WebhookInputTests(_LiveServer):
    def _add(self, series_id):
        return self.post({"eventType": "SeriesAdd", "series": {"id": series_id}},
                         headers=self.basic(SECRET))

    def test_non_integer_ids_rejected(self):
        for bad in ["x" * 5000, True, 1.5, -1, 0, 2**40, None, [1]]:
            status, _ = self._add(bad)
            self.assertEqual(status, 400, f"id={bad!r}")
        scheduler.webhook_executor.submit.assert_not_called()

    def test_duplicate_series_is_deduplicated(self):
        self.assertEqual(self._add(7)[1]["status"], "triggered")
        self.assertEqual(self._add(7)[1]["status"], "already_queued")
        self.assertEqual(scheduler.webhook_executor.submit.call_count, 1)

    def test_queue_bound_returns_503(self):
        with patch.object(scheduler, "MAX_PENDING_WEBHOOKS", 3):
            for sid in (1, 2, 3):
                self.assertEqual(self._add(sid)[0], 200)
            status, data = self._add(4)
        self.assertEqual(status, 503)
        self.assertEqual(data["status"], "busy")

    def test_pending_slot_released_after_processing(self):
        self._add(9)
        with patch.object(scheduler, "run_webhook_script") as run:
            scheduler.process_webhook_series(9)
        run.assert_called_once_with(9)
        self.assertNotIn(9, scheduler._pending_series)

    def test_non_object_json_is_400(self):
        status, _ = self.post(b"[1,2,3]", headers=self.basic(SECRET))
        self.assertEqual(status, 400)

    def test_log_injection_is_escaped(self):
        title = "Real\n2026/01/01 00:00:00 INFO forged line\x1b[2K"
        with self.assertLogs(scheduler.log, level="INFO") as cm:
            self.post({"eventType": "Grab", "series": {"id": 5, "title": title}},
                      headers=self.basic(SECRET))
        joined = "\n".join(cm.output)
        self.assertNotIn("\nforged", joined.replace("\\n", ""))
        self.assertNotIn("\x1b", joined)
        self.assertTrue(all("forged line" not in line or "Real\\n" in line for line in cm.output))


class ConnectionCapTests(_LiveServer):
    max_connections = 2

    def test_connections_beyond_cap_are_dropped(self):
        held = [socket.create_connection(("127.0.0.1", self.port)) for _ in range(2)]
        try:
            time.sleep(0.2)  # let the server accept and park both
            extra = socket.create_connection(("127.0.0.1", self.port))
            extra.settimeout(3)
            try:
                self.assertEqual(extra.recv(1), b"")  # closed without service
            except ConnectionResetError:
                pass
            finally:
                extra.close()
        finally:
            for s in held:
                s.close()

    def test_slot_freed_after_connection_finishes(self):
        for _ in range(5):  # more sequential requests than slots
            status, _ = self.post({"eventType": "Test"}, headers=self.basic(SECRET))
            self.assertEqual(status, 200)


class HeaderBudgetTests(_LiveServer):
    def test_oversized_headers_get_431_without_buffering(self):
        """Stock http.server would buffer ~6.5 MB of headers per connection; the
        budget cuts it off at MAX_HEADER_BYTES and answers 431."""
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            sock.sendall(b"POST / HTTP/1.1\r\nHost: x\r\n")
            try:
                for i in range(40):
                    sock.sendall(f"X-Pad-{i}: ".encode() + b"A" * 60_000 + b"\r\n")
                sock.sendall(b"\r\n")
            except OSError:
                pass  # server stopped reading once the budget was spent
            reply = b""
            try:
                while chunk := sock.recv(4096):
                    reply += chunk
            except OSError:
                pass
        finally:
            sock.close()
        self.assertIn(b" 431 ", reply.split(b"\r\n", 1)[0])
        scheduler.webhook_executor.submit.assert_not_called()

    def test_normal_headers_unaffected(self):
        status, _ = self.post({"eventType": "Test"}, headers=self.basic(SECRET))
        self.assertEqual(status, 200)


class ConnectionDeadlineTests(_LiveServer):
    def test_slow_drip_client_is_cut_off_at_deadline(self):
        with patch.object(scheduler, "WEBHOOK_REQUEST_TIMEOUT", 1), \
             patch.object(scheduler.WebhookHandler, "timeout", 5):
            sock = socket.create_connection(("127.0.0.1", self.port))
            sock.settimeout(5)
            start = time.monotonic()
            closed = False
            try:
                sock.sendall(b"POST / HTTP/1.1\r\n")
                for _ in range(20):  # drip one header byte every 0.3s
                    time.sleep(0.3)
                    try:
                        sock.sendall(b"X")
                    except OSError:
                        closed = True
                        break
                    sock.setblocking(False)
                    try:
                        if sock.recv(1024) == b"":
                            closed = True
                            break
                    except BlockingIOError:
                        pass
                    except OSError:
                        closed = True
                        break
                    finally:
                        sock.setblocking(True)
                        sock.settimeout(5)
            finally:
                sock.close()
        self.assertTrue(closed, "server never tore down the slow-drip connection")
        self.assertLess(time.monotonic() - start, 4)


class AuthHelperTests(unittest.TestCase):
    def test_unset_secret_is_403(self):
        self.assertEqual(scheduler.check_webhook_auth({}, "/", secret=""), 403)

    def test_bearer_and_username_forms(self):
        self.assertIsNone(scheduler.check_webhook_auth(
            {"Authorization": "Bearer abc"}, "/", secret="abc"))
        token = base64.b64encode(b"abc:").decode()
        self.assertIsNone(scheduler.check_webhook_auth(
            {"Authorization": f"Basic {token}"}, "/", secret="abc"))

    def test_malformed_basic_header_is_401(self):
        self.assertEqual(scheduler.check_webhook_auth(
            {"Authorization": "Basic !!!notbase64"}, "/", secret="abc"), 401)


class PlaybackWatchdogTests(unittest.TestCase):
    def setUp(self):
        self._saved = dict(scheduler._playback_state)
        self.addCleanup(lambda: scheduler._playback_state.update(self._saved))

    def test_stalled_only_after_threshold(self):
        now = 10_000.0
        scheduler._playback_state.update(heartbeat=now, disabled=False)
        self.assertFalse(scheduler.playback_loop_stalled(now + 1))
        self.assertTrue(scheduler.playback_loop_stalled(now + scheduler.PLAYBACK_STALL_SECONDS + 1))

    def test_disabled_loop_never_reports_stalled(self):
        scheduler._playback_state.update(heartbeat=0.0, disabled=True)
        self.assertFalse(scheduler.playback_loop_stalled(1e9))

    def test_replaced_loop_exits(self):
        """A loop whose generation was superseded stops instead of running in
        parallel with its replacement."""
        calls = []

        def fake_check(conn, session=None, stop_event=None):
            calls.append(1)
            with scheduler._playback_state_lock:
                scheduler._playback_state["generation"] += 1  # watchdog replaced us

        with patch.object(scheduler.media_automation, "init_db", return_value=MagicMock()), \
             patch.object(scheduler.media_automation, "check_db_writable", return_value=True), \
             patch.object(scheduler.media_automation, "check_active_playback", side_effect=fake_check), \
             patch.object(scheduler.shutdown_event, "wait", return_value=False):
            with scheduler._playback_state_lock:
                gen = scheduler._playback_state["generation"]
            scheduler.playback_check_loop(gen)
        self.assertEqual(len(calls), 1)

    def test_health_endpoint_503_when_stalled(self):
        handler = scheduler.WebhookHandler.__new__(scheduler.WebhookHandler)
        responses = []
        handler._respond = lambda code, data, extra_headers=None: responses.append(code)
        with patch.object(scheduler, "playback_loop_stalled", return_value=True):
            handler.do_GET()
        with patch.object(scheduler, "playback_loop_stalled", return_value=False):
            handler.do_GET()
        self.assertEqual(responses, [503, 200])


class SecurityUtilsTests(unittest.TestCase):
    def test_redact_userinfo_and_query_secrets(self):
        out = security_utils.redact("http://admin:pw@sonarr:8989/api?apikey=K1&api_key=K2&token=T&mode=q")
        for secret in ("admin", ":pw", "K1", "K2", "=T"):
            self.assertNotIn(secret, out)
        self.assertIn("mode=q", out)

    def test_redact_exception_keeps_type(self):
        import requests
        exc = requests.exceptions.ConnectionError("boom https://x/3?api_key=SECRET")
        security_utils.redact_exception(exc)
        self.assertIsInstance(exc, requests.exceptions.ConnectionError)
        self.assertNotIn("SECRET", str(exc))

    def test_clamp_retry_after(self):
        self.assertEqual(security_utils.clamp_retry_after("-5"), (0, True))
        self.assertEqual(security_utils.clamp_retry_after("9" * 30), (120, True))
        self.assertEqual(security_utils.clamp_retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), (60, False))

    def test_safe_log_and_prompt_sanitizer(self):
        self.assertNotIn("\n", security_utils.safe_log("a\nb"))
        self.assertLess(len(security_utils.safe_log("x" * 10_000)), 260)
        self.assertEqual(security_utils.sanitize_prompt_text("A\nSYSTEM: obey\x00"), "A SYSTEM: obey")


if __name__ == "__main__":
    unittest.main()
