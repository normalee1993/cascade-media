"""Tests for the alert delivery path and the `test-alert --verify` mode (Phase 5).

A silent alert failure once let watched items get re-downloaded for 9 days, so
the delivery path earns a real end-to-end check. These cover the fan-out to both
channels, per-channel ✓/✗ status reporting, the --verify exit code, and proof
that the existing fire-and-forget call sites (token-failure / persistence /
readonly alerts) behave exactly as before.

No real network or SMTP — requests.post and smtplib are faked.
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trakt_discovery


def _resp(status_code):
    """A fake requests.Response with just the status_code the code reads."""
    r = MagicMock()
    r.status_code = status_code
    return r


def _alert_config(webhook=None, email=False):
    """Context-manager bundle patching the module-level alert config.

    webhook: the ALERT_WEBHOOK_URL value (None/'' → webhook unconfigured).
    email:   True → set the SMTP_HOST + ALERT_EMAIL_* trio so email is configured.
    """
    patches = [
        patch.object(trakt_discovery, "ALERT_WEBHOOK_URL", webhook or ""),
        patch.object(trakt_discovery, "SMTP_HOST", "smtp.example.com" if email else ""),
        patch.object(trakt_discovery, "ALERT_EMAIL_TO", "to@example.com" if email else ""),
        patch.object(trakt_discovery, "ALERT_EMAIL_FROM", "from@example.com" if email else ""),
        patch.object(trakt_discovery, "SMTP_PORT", 587),
        patch.object(trakt_discovery, "SMTP_USE_TLS", True),
        patch.object(trakt_discovery, "SMTP_USER", ""),
        patch.object(trakt_discovery, "SMTP_PASS", ""),
    ]
    return patches


class _PatchBundle:
    """Start/stop a list of patchers as one context manager."""

    def __init__(self, patches):
        self._patches = patches

    def __enter__(self):
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()
        return False


class FanOutTests(unittest.TestCase):
    """_send_alert_webhook fans out to every configured channel."""

    def test_both_channels_attempted_when_both_configured(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=True)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(204)) as post, \
             patch("smtplib.SMTP") as smtp:
            results = trakt_discovery._send_alert_webhook("msg", subject="Test alert")
        post.assert_called_once()
        smtp.assert_called_once()
        self.assertEqual(results["webhook"], (True, 204))
        self.assertEqual(results["email"], (True, "sent"))

    def test_only_webhook_attempted_when_only_webhook_configured(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=False)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(200)) as post, \
             patch("smtplib.SMTP") as smtp:
            results = trakt_discovery._send_alert_webhook("msg")
        post.assert_called_once()
        smtp.assert_not_called()
        self.assertEqual(results["webhook"], (True, 200))
        self.assertIsNone(results["email"])

    def test_only_email_attempted_when_only_email_configured(self):
        with _PatchBundle(_alert_config(webhook=None, email=True)), \
             patch.object(trakt_discovery.requests, "post") as post, \
             patch("smtplib.SMTP") as smtp:
            results = trakt_discovery._send_alert_webhook("msg")
        post.assert_not_called()
        smtp.assert_called_once()
        self.assertIsNone(results["webhook"])
        self.assertEqual(results["email"], (True, "sent"))


class PerChannelStatusTests(unittest.TestCase):
    """A 2xx → ok; a non-2xx or transport exception → not ok."""

    def test_webhook_2xx_is_success(self):
        with _PatchBundle(_alert_config(webhook="https://hook")), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(204)):
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["webhook"], (True, 204))

    def test_webhook_non_2xx_is_failure_with_status(self):
        with _PatchBundle(_alert_config(webhook="https://hook")), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(500)):
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["webhook"], (False, 500))

    def test_webhook_exception_is_failure_no_status(self):
        with _PatchBundle(_alert_config(webhook="https://hook")), \
             patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.ConnectionError("boom")):
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["webhook"], (False, None))

    def test_email_success(self):
        with _PatchBundle(_alert_config(email=True)), \
             patch("smtplib.SMTP"):
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["email"], (True, "sent"))

    def test_email_exception_is_failure(self):
        with _PatchBundle(_alert_config(email=True)), \
             patch("smtplib.SMTP", side_effect=OSError("connect refused")):
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["email"], (False, "error"))


class VerifyExitCodeTests(unittest.TestCase):
    """cmd_test_alert(verify=True) returns False iff a configured channel failed."""

    def test_all_success_returns_true(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=True)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(200)), \
             patch("smtplib.SMTP"):
            out = io.StringIO()
            with redirect_stdout(out):
                ok = trakt_discovery.cmd_test_alert(None, verify=True)
        self.assertTrue(ok)
        text = out.getvalue()
        self.assertIn("✓ webhook (200)", text)
        self.assertIn("✓ email→to@example.com (sent)", text)

    def test_webhook_failure_returns_false(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=True)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(503)), \
             patch("smtplib.SMTP"):
            out = io.StringIO()
            with redirect_stdout(out):
                ok = trakt_discovery.cmd_test_alert(None, verify=True)
        self.assertFalse(ok)
        self.assertIn("✗ webhook (503)", out.getvalue())

    def test_email_failure_returns_false(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=True)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(200)), \
             patch("smtplib.SMTP", side_effect=OSError("nope")):
            out = io.StringIO()
            with redirect_stdout(out):
                ok = trakt_discovery.cmd_test_alert(None, verify=True)
        self.assertFalse(ok)
        text = out.getvalue()
        self.assertIn("✓ webhook (200)", text)
        self.assertIn("✗ email→to@example.com (error)", text)

    def test_unconfigured_channel_not_judged(self):
        """Only configured channels affect the exit code; an unset channel is
        skipped, not counted as a failure."""
        with _PatchBundle(_alert_config(webhook="https://hook", email=False)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(200)):
            out = io.StringIO()
            with redirect_stdout(out):
                ok = trakt_discovery.cmd_test_alert(None, verify=True)
        self.assertTrue(ok)
        text = out.getvalue()
        self.assertIn("✓ webhook (200)", text)
        self.assertNotIn("email", text)

    def test_no_channels_configured_returns_false(self):
        with _PatchBundle(_alert_config(webhook=None, email=False)), \
             patch.object(trakt_discovery.requests, "post") as post, \
             patch("smtplib.SMTP") as smtp:
            ok = trakt_discovery.cmd_test_alert(None, verify=True)
        self.assertFalse(ok)
        post.assert_not_called()
        smtp.assert_not_called()

    def test_no_channels_configured_without_verify_also_false(self):
        """Current behavior: no channels → False regardless of --verify."""
        with _PatchBundle(_alert_config(webhook=None, email=False)):
            self.assertFalse(trakt_discovery.cmd_test_alert(None, verify=False))


class NonVerifyBehaviorTests(unittest.TestCase):
    """The fire-and-forget path (and all existing alert call sites) is unchanged."""

    def test_non_verify_returns_true_and_sends_without_printing(self):
        with _PatchBundle(_alert_config(webhook="https://hook", email=True)), \
             patch.object(trakt_discovery.requests, "post", return_value=_resp(200)) as post, \
             patch("smtplib.SMTP") as smtp:
            out = io.StringIO()
            with redirect_stdout(out):
                ok = trakt_discovery.cmd_test_alert(None)  # verify defaults to False
        self.assertTrue(ok)
        post.assert_called_once()
        smtp.assert_called_once()
        # No per-channel summary printed in fire-and-forget mode.
        self.assertEqual(out.getvalue(), "")

    def test_non_verify_true_even_when_webhook_fails(self):
        """Fire-and-forget never fails on a bad send — that's the legacy contract
        the token-failure/persistence/readonly callers rely on."""
        with _PatchBundle(_alert_config(webhook="https://hook")), \
             patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.ConnectionError("boom")):
            self.assertTrue(trakt_discovery.cmd_test_alert(None, verify=False))

    def test_send_alert_once_dedups_per_process(self):
        """The existing call sites go through _send_alert_once for dedup; a second
        call with the same flag must NOT re-send (the 2026-05-24 email-storm guard)."""
        trakt_discovery._token_cache.clear()
        try:
            with patch.object(trakt_discovery, "_send_alert_webhook") as send:
                trakt_discovery._send_alert_once("test_flag", "msg", subject="X")
                trakt_discovery._send_alert_once("test_flag", "msg", subject="X")
            send.assert_called_once_with("msg", subject="X")
        finally:
            trakt_discovery._token_cache.clear()

    def test_webhook_exception_does_not_propagate(self):
        """A transport failure must be swallowed (callers never wrap it)."""
        with _PatchBundle(_alert_config(webhook="https://hook")), \
             patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.ConnectionError("boom")):
            # Must not raise.
            results = trakt_discovery._send_alert_webhook("msg")
        self.assertEqual(results["webhook"], (False, None))


if __name__ == "__main__":
    unittest.main()
