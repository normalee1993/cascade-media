"""Regression tests for the bug classes that bit production in 2026-05.

Each test covers a specific incident class — comments name the dates so future
maintainers can trace which real-world failure each guard exists to prevent.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trakt_discovery


class NetworkFilterTests(unittest.TestCase):
    """TMDB_DISALLOWED_NETWORKS — 2026-05-11 Apple TV+ rebrand + HBO/Max split."""

    def setUp(self):
        # check_tmdb_filters short-circuits if TMDB_API_KEY is empty
        self._key_patch = patch.object(trakt_discovery, "TMDB_API_KEY", "test-key")
        self._key_patch.start()

    def tearDown(self):
        self._key_patch.stop()

    def test_apple_tv_blocked_after_rebrand(self):
        """TMDB renamed 'Apple TV+' → 'Apple TV'. The rebrand silently broke the
        exact-string filter on 2026-05-11, leaking For All Mankind, Monarch, etc."""
        with patch.object(trakt_discovery, "TMDB_DISALLOWED_NETWORKS", ["Apple TV", "Max"]), \
             patch.object(trakt_discovery, "fetch_tmdb_details",
                          return_value={"networks": [{"name": "Apple TV"}]}):
            passed, reason = trakt_discovery.check_tmdb_filters("show", 1, "For All Mankind")
        self.assertFalse(passed)
        self.assertEqual(reason, "skipped_disallowed_network")

    def test_hbo_blocked_separately_from_max(self):
        """TMDB lists HBO and Max as separate networks. A user with only 'Max' in
        the deny list leaked HBO originals (Rooster, Task, IT: Welcome to Derry)."""
        with patch.object(trakt_discovery, "TMDB_DISALLOWED_NETWORKS", ["HBO", "Max"]), \
             patch.object(trakt_discovery, "fetch_tmdb_details",
                          return_value={"networks": [{"name": "HBO"}]}):
            passed, reason = trakt_discovery.check_tmdb_filters("show", 2, "Task")
        self.assertFalse(passed)
        self.assertEqual(reason, "skipped_disallowed_network")

    def test_unrelated_network_passes(self):
        with patch.object(trakt_discovery, "TMDB_DISALLOWED_NETWORKS", ["Apple TV", "Max"]), \
             patch.object(trakt_discovery, "fetch_tmdb_details",
                          return_value={"networks": [{"name": "FX"}]}):
            passed, reason = trakt_discovery.check_tmdb_filters("show", 3, "Shogun")
        self.assertTrue(passed)
        self.assertIsNone(reason)


class ProviderFilterTests(unittest.TestCase):
    """TMDB_DISALLOWED_PROVIDERS — the Watch Providers filter (2026-05-11 structural fix).

    Catches streaming distributors the network filter misses (Bodyguard class:
    BBC origination + Netflix distribution)."""

    def setUp(self):
        self._patches = [
            patch.object(trakt_discovery, "TMDB_API_KEY", "test-key"),
            patch.object(trakt_discovery, "TMDB_DISALLOWED_NETWORKS", []),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()

    def test_apple_tv_plus_provider_blocked_for_shows(self):
        """startswith match — 'Apple TV' catches both 'Apple TV+' and 'Apple TV Amazon Channel'."""
        with patch.object(trakt_discovery, "TMDB_DISALLOWED_PROVIDERS", ["Apple TV"]), \
             patch.object(trakt_discovery, "fetch_tmdb_details", return_value={"id": 1}), \
             patch.object(trakt_discovery, "fetch_tmdb_watch_providers",
                          return_value=["Apple TV+"]):
            passed, reason = trakt_discovery.check_tmdb_filters("show", 1, "Foundation")
        self.assertFalse(passed)
        self.assertEqual(reason, "skipped_disallowed_provider")

    def test_provider_filter_does_not_apply_to_movies(self):
        """Movies bypass the provider filter by design — theatrical releases often
        have higher-quality non-streaming versions even when also on a service."""
        with patch.object(trakt_discovery, "TMDB_DISALLOWED_PROVIDERS", ["Apple TV"]), \
             patch.object(trakt_discovery, "fetch_tmdb_details", return_value={"id": 1}), \
             patch.object(trakt_discovery, "fetch_tmdb_watch_providers",
                          return_value=["Apple TV+"]) as m:
            passed, reason = trakt_discovery.check_tmdb_filters("movie", 1, "F1")
        self.assertTrue(passed)
        self.assertIsNone(reason)
        m.assert_not_called()


class WatchedHistorySafetyNetTests(unittest.TestCase):
    """PR #1, 2026-05-12: fetch_watched_ids returns None per media type on fetch
    failure. Caller treats None as 'unknown — block requests'."""

    def test_returns_none_on_fetch_failure(self):
        """If Trakt API returns anything non-list (auth break, 5xx) we must return
        None for that media type. Empty set would falsely report 'nothing watched'
        and let already-watched items through."""
        with patch.object(trakt_discovery, "trakt_get", return_value=None):
            watched = trakt_discovery.fetch_watched_ids(conn=None)
        self.assertIsNone(watched["show"])
        self.assertIsNone(watched["movie"])

    def test_returns_set_on_successful_fetch(self):
        def fake_trakt_get(endpoint, **kw):
            if "shows" in endpoint:
                return [{"show": {"ids": {"trakt": 100}}}, {"show": {"ids": {"trakt": 101}}}]
            if "movies" in endpoint:
                return [{"movie": {"ids": {"trakt": 200}}}]
            return None

        with patch.object(trakt_discovery, "trakt_get", side_effect=fake_trakt_get):
            watched = trakt_discovery.fetch_watched_ids(conn=None)
        self.assertEqual(watched["show"], {100, 101})
        self.assertEqual(watched["movie"], {200})


class MissingTokensAlertTests(unittest.TestCase):
    """PR #3, 2026-05-16: get_valid_token must fire the alert webhook when tokens
    are missing entirely — not just on refresh failure. Closed the alert gap that
    caused 2 days of silent zero-discovery on 2026-05-15..16."""

    def setUp(self):
        trakt_discovery._token_cache.clear()

    def test_missing_tokens_fires_alert_once_per_run(self):
        """Empty trakt_tokens table → alert webhook fires once. Subsequent calls in
        the same run must NOT spam (missing_alert_fired guard)."""
        with patch.object(trakt_discovery, "load_tokens", return_value=None), \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            first = trakt_discovery.get_valid_token(conn=None)
            second = trakt_discovery.get_valid_token(conn=None)
        self.assertIsNone(first)
        self.assertIsNone(second)
        mock_alert.assert_called_once()


class RefreshTokenPersistenceTests(unittest.TestCase):
    """2026-05-22 incident: refresh_access_token's try/except only caught
    RequestException. When save_tokens raised OperationalError (readonly DB),
    the exception propagated silently — Trakt had already rotated the
    refresh_token server-side, so the new token was lost and the next refresh
    on 05-23 hit a 400 Bad Request with the stale refresh_token."""

    def setUp(self):
        trakt_discovery._token_cache.clear()

    def _fake_response(self):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 7 * 86400,
        }
        return r

    def test_save_failure_returns_none_and_alerts(self):
        """HTTP refresh succeeds, save_tokens raises OperationalError → must
        return None AND fire alert. Without this, the caller treats the in-memory
        access_token as good for the rest of the run while Trakt has invalidated
        the refresh_token server-side."""
        with patch.object(trakt_discovery.requests, "post", return_value=self._fake_response()), \
             patch.object(trakt_discovery, "save_tokens",
                          side_effect=sqlite3.OperationalError("attempt to write a readonly database")), \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            result = trakt_discovery.refresh_access_token(conn=None, refresh_token="old-refresh")
        self.assertIsNone(result)
        mock_alert.assert_called_once()
        # Subject distinguishes persistence failure from HTTP failure
        self.assertEqual(mock_alert.call_args.kwargs.get("subject"), "Token persistence failure")

    def test_http_failure_returns_none_and_alerts(self):
        """RequestException path still works — covered for regression."""
        with patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.RequestException("400 Bad Request")), \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            result = trakt_discovery.refresh_access_token(conn=None, refresh_token="stale-refresh")
        self.assertIsNone(result)
        mock_alert.assert_called_once()

    def test_successful_refresh_returns_access_token(self):
        """Happy path — HTTP succeeds, save succeeds, returns access_token, no alert."""
        with patch.object(trakt_discovery.requests, "post", return_value=self._fake_response()), \
             patch.object(trakt_discovery, "save_tokens") as mock_save, \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            result = trakt_discovery.refresh_access_token(conn=None, refresh_token="old-refresh")
        self.assertEqual(result, "new-access")
        mock_save.assert_called_once()
        mock_alert.assert_not_called()


class AlertDedupTests(unittest.TestCase):
    """2026-05-24 incident: 8+ duplicate alert emails in 25 seconds. Both
    fetch_watched_ids(shows) and fetch_watched_ids(movies) independently call
    get_valid_token, and refresh failures inside each path fired alerts directly
    (no dedup). _send_alert_once mirrors the missing_alert_fired guard."""

    def setUp(self):
        trakt_discovery._token_cache.clear()

    def test_refresh_failure_alerts_once_across_multiple_calls(self):
        """Same process, multiple refresh attempts with HTTP failure → 1 alert."""
        with patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.RequestException("400")), \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            trakt_discovery.refresh_access_token(conn=None, refresh_token="x")
            trakt_discovery.refresh_access_token(conn=None, refresh_token="x")
            trakt_discovery.refresh_access_token(conn=None, refresh_token="x")
        mock_alert.assert_called_once()

    def test_expired_alert_fires_once_per_run(self):
        """get_valid_token with expired token + failing refresh fires expired
        alert at most once per process."""
        # Token expired yesterday
        expired_iso = (trakt_discovery.datetime.now(trakt_discovery.timezone.utc)
                       - trakt_discovery.timedelta(days=1)).isoformat()
        fake_tokens = {"access_token": "old", "refresh_token": "old-r", "expires_at": expired_iso}
        with patch.object(trakt_discovery, "load_tokens", return_value=fake_tokens), \
             patch.object(trakt_discovery, "refresh_access_token", return_value=None), \
             patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
            trakt_discovery.get_valid_token(conn=None)
            trakt_discovery._token_cache.pop("token", None)  # force re-check
            trakt_discovery._token_cache.pop("cached_at", None)
            trakt_discovery.get_valid_token(conn=None)
        mock_alert.assert_called_once()


class DbWritabilityProbeTests(unittest.TestCase):
    """2026-05-22 root cause: bind-mount DB was root-owned, container ran as
    uid 99. SQLite opened read-only and the state only surfaced inside
    refresh_access_token. check_db_writable surfaces it at cmd_discover
    startup, before any state can be consumed."""

    def setUp(self):
        trakt_discovery._token_cache.clear()
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "probe.db")
        # Seed with a real DB so the readonly open has a valid file
        seed = sqlite3.connect(self._db_path)
        seed.execute("CREATE TABLE t (x INTEGER)")
        seed.commit()
        seed.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_writable_db_passes(self):
        conn = sqlite3.connect(self._db_path)
        try:
            self.assertTrue(trakt_discovery.check_db_writable(conn))
        finally:
            conn.close()

    def test_readonly_db_fails_and_alerts(self):
        """Open via URI in mode=ro. Probe must return False + fire alert."""
        conn = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            with patch.object(trakt_discovery, "_send_alert_webhook") as mock_alert:
                self.assertFalse(trakt_discovery.check_db_writable(conn))
                # Second probe in same process must not double-alert
                self.assertFalse(trakt_discovery.check_db_writable(conn))
            mock_alert.assert_called_once()
            # Message names the chown command so the alert is actionable
            msg = mock_alert.call_args[0][0]
            self.assertIn("chown 99:users", msg)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
