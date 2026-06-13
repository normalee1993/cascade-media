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

import media_automation
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


class CascadeTargetSeasonTests(unittest.TestCase):
    """determine_target_season — which season(s) get the full download.

    Encodes the priority documented in SYSTEM_DOCUMENTATION.md:
      1. Seerr specific seasons → minimum requested + that set
      2. Seerr "all seasons" → Season 1 only
      3. Seerr "remaining seasons" (empty list) → lowest season without files
      4. No Seerr, files exist → lowest season with files
      5. No data → Season 1
    """

    @staticmethod
    def _episodes(seasons_with_counts, seasons_with_files=()):
        """Build a fake episodes list: {season_number: episode_count}."""
        eps = []
        ep_id = 1
        for sn, count in seasons_with_counts.items():
            for en in range(1, count + 1):
                eps.append({
                    "id": ep_id,
                    "seasonNumber": sn,
                    "episodeNumber": en,
                    "monitored": True,
                    "hasFile": sn in seasons_with_files,
                })
                ep_id += 1
        return eps

    def test_all_seasons_requested_targets_s1_only(self):
        """Killer Cases case: Seerr returns full season list → target S1 only.
        Without this, the cascade would request every season and the preview
        logic would never run."""
        series = {"title": "Killer Cases", "tvdbId": 12345}
        episodes = self._episodes({1: 10, 2: 10, 3: 12, 4: 12, 5: 8, 6: 6, 7: 8})
        with patch.object(media_automation, "get_requested_seasons_from_seerr",
                          return_value={1, 2, 3, 4, 5, 6, 7}):
            target, target_seasons = media_automation.determine_target_season(series, episodes)
        self.assertEqual(target, 1)
        self.assertEqual(target_seasons, {1})

    def test_specific_seasons_requested_targets_those(self):
        """Partial-season request: only those seasons go full, everything else
        gets the E01 preview treatment."""
        series = {"title": "Show", "tvdbId": 100}
        episodes = self._episodes({1: 10, 2: 10, 3: 12, 4: 12})
        with patch.object(media_automation, "get_requested_seasons_from_seerr",
                          return_value={3, 4}):
            target, target_seasons = media_automation.determine_target_season(series, episodes)
        self.assertEqual(target, 3)
        self.assertEqual(target_seasons, {3, 4})

    def test_seerr_remaining_seasons_uses_lowest_unfilled(self):
        """Empty seasons list from Seerr (the 'remaining seasons' UI option)
        means 'whatever's missing'. Pick the lowest season without files."""
        series = {"title": "Show", "tvdbId": 100}
        episodes = self._episodes({1: 10, 2: 10, 3: 12}, seasons_with_files={1})
        with patch.object(media_automation, "get_requested_seasons_from_seerr",
                          return_value=set()):
            target, target_seasons = media_automation.determine_target_season(series, episodes)
        self.assertEqual(target, 2)
        self.assertEqual(target_seasons, {2})

    def test_no_seerr_with_existing_files_uses_lowest_with_files(self):
        """When Seerr doesn't know about the show (None) but files already exist,
        treat the lowest-numbered season-with-files as the target."""
        series = {"title": "Show", "tvdbId": 100}
        episodes = self._episodes({1: 10, 2: 10, 3: 12}, seasons_with_files={2, 3})
        with patch.object(media_automation, "get_requested_seasons_from_seerr",
                          return_value=None):
            target, target_seasons = media_automation.determine_target_season(series, episodes)
        self.assertEqual(target, 2)
        self.assertEqual(target_seasons, {2, 3})

    def test_no_seerr_no_files_defaults_to_s1(self):
        series = {"title": "Show", "tvdbId": 100}
        episodes = self._episodes({1: 10, 2: 10, 3: 12})
        with patch.object(media_automation, "get_requested_seasons_from_seerr",
                          return_value=None):
            target, target_seasons = media_automation.determine_target_season(series, episodes)
        self.assertEqual(target, 1)
        self.assertEqual(target_seasons, {1})


class CascadeApplyMonitoringTests(unittest.TestCase):
    """apply_monitoring — the per-episode monitor flips that gate Sonarr search.

    2026-05-25 Killer Cases incident: the original implementation looped
    sonarr_put(f"/episode/{id}", ep) for every episode, taking ~4 seconds.
    Sonarr's auto-search-on-add enumerated episode IDs at T≈0.1s and had
    pushed NZBs to SABnzbd before that loop could complete. The fix collapses
    the loop to two bulk PUT /episode/monitor calls plus one PUT /series/{id}
    season-level update — issued in that order, with the series PUT first so
    season-level monitor=False stops Sonarr's auto-search before any episode
    work begins."""

    def _series_detail(self, all_seasons):
        return {
            "id": 1,
            "title": "Show",
            "seasons": [{"seasonNumber": sn, "monitored": True} for sn in sorted(all_seasons)],
        }

    @staticmethod
    def _episodes(seasons_with_counts, currently_monitored=True):
        """Episodes whose monitored flag matches Sonarr's default-on-add (all True)."""
        eps = []
        ep_id = 1
        for sn, count in seasons_with_counts.items():
            for en in range(1, count + 1):
                eps.append({
                    "id": ep_id,
                    "seasonNumber": sn,
                    "episodeNumber": en,
                    "monitored": currently_monitored,
                    "hasFile": False,
                })
                ep_id += 1
        return eps

    def test_uses_bulk_endpoint_not_per_episode_loop(self):
        """For the 56-episode Killer Cases case, the new code should issue:
          - 1× PUT /series/{id} (season-level flags)
          - 1× PUT /episode/monitor (unmonitor list)
          - 0–1× PUT /episode/monitor (monitor list — empty if previews already on)
        and ZERO calls to /episode/{id}. The whole point of the fix."""
        all_seasons = {1, 2, 3, 4, 5, 6, 7}
        episodes = self._episodes({1: 10, 2: 10, 3: 12, 4: 12, 5: 8, 6: 6, 7: 8})
        series_detail = self._series_detail(all_seasons)

        with patch.object(media_automation, "DRY_RUN", False), \
             patch.object(media_automation, "sonarr_get", return_value=series_detail), \
             patch.object(media_automation, "sonarr_put") as mock_put:
            media_automation.apply_monitoring(
                series_id=1, title="Killer Cases", episodes=episodes,
                target_seasons={1}, all_seasons=all_seasons,
            )

        put_calls = mock_put.call_args_list
        endpoints = [c.args[0] for c in put_calls]
        # No per-episode PUTs
        self.assertFalse(
            any(ep.startswith("/episode/") and ep != "/episode/monitor" for ep in endpoints),
            f"unexpected per-episode PUTs: {endpoints}",
        )
        # Series-level update happened exactly once
        self.assertEqual(endpoints.count("/series/1"), 1)
        # At least one bulk monitor call (the unmonitor list — 50 unwanted episodes)
        self.assertGreaterEqual(endpoints.count("/episode/monitor"), 1)
        self.assertLessEqual(endpoints.count("/episode/monitor"), 2)

    def test_series_put_fires_before_episode_monitor(self):
        """Ordering matters: season-level monitor=False on S2-7 must hit Sonarr
        before any episode flip, so Sonarr's auto-search is gated as early as
        possible."""
        all_seasons = {1, 2, 3, 4}
        episodes = self._episodes({1: 5, 2: 5, 3: 5, 4: 5})

        with patch.object(media_automation, "DRY_RUN", False), \
             patch.object(media_automation, "sonarr_get",
                          return_value=self._series_detail(all_seasons)), \
             patch.object(media_automation, "sonarr_put") as mock_put:
            media_automation.apply_monitoring(
                series_id=1, title="Show", episodes=episodes,
                target_seasons={1}, all_seasons=all_seasons,
            )

        endpoints = [c.args[0] for c in mock_put.call_args_list]
        series_idx = endpoints.index("/series/1")
        monitor_indices = [i for i, e in enumerate(endpoints) if e == "/episode/monitor"]
        self.assertTrue(monitor_indices, "expected at least one /episode/monitor call")
        self.assertLess(series_idx, min(monitor_indices),
                        f"series PUT must precede all episode/monitor PUTs; got {endpoints}")

    def test_bulk_unmonitor_payload_targets_correct_episodes(self):
        """The unmonitor list must contain only the to-be-skipped episodes
        (S2E02+, S3E02+, S4E02+) — never S1 episodes, never any E01 of a
        non-target season."""
        all_seasons = {1, 2, 3, 4}
        # S1: 3 eps (all should stay monitored), S2-4: 4 eps each (E01 stays, E02-4 unmonitor)
        episodes = self._episodes({1: 3, 2: 4, 3: 4, 4: 4})
        episode_by_id = {ep["id"]: ep for ep in episodes}

        with patch.object(media_automation, "DRY_RUN", False), \
             patch.object(media_automation, "sonarr_get",
                          return_value=self._series_detail(all_seasons)), \
             patch.object(media_automation, "sonarr_put") as mock_put:
            media_automation.apply_monitoring(
                series_id=1, title="Show", episodes=episodes,
                target_seasons={1}, all_seasons=all_seasons,
            )

        monitor_calls = [c for c in mock_put.call_args_list if c.args[0] == "/episode/monitor"]
        unmonitor_call = next((c for c in monitor_calls if c.args[1]["monitored"] is False), None)
        self.assertIsNotNone(unmonitor_call, "expected an unmonitor bulk call")
        unmonitored_ids = set(unmonitor_call.args[1]["episodeIds"])

        for ep_id in unmonitored_ids:
            ep = episode_by_id[ep_id]
            self.assertNotEqual(ep["seasonNumber"], 1, f"S1 ep {ep_id} should never be unmonitored")
            self.assertNotEqual(ep["episodeNumber"], 1,
                                f"E01 of any season should never be unmonitored (ep {ep_id})")
        # Expected count: S2-4 each have 3 unwanted episodes (E02, E03, E04) = 9 total
        self.assertEqual(len(unmonitored_ids), 9)

    def test_no_changes_means_no_bulk_calls(self):
        """If episodes are already in their target state, only the series PUT
        fires — no /episode/monitor calls. Avoids unnecessary API traffic in
        the 15s/10s/20s/30s re-apply passes."""
        all_seasons = {1, 2}
        # Episode state already matches what we want: S1 all monitored, S2 only E01.
        episodes = [
            {"id": 1, "seasonNumber": 1, "episodeNumber": 1, "monitored": True, "hasFile": False},
            {"id": 2, "seasonNumber": 1, "episodeNumber": 2, "monitored": True, "hasFile": False},
            {"id": 3, "seasonNumber": 2, "episodeNumber": 1, "monitored": True, "hasFile": False},
            {"id": 4, "seasonNumber": 2, "episodeNumber": 2, "monitored": False, "hasFile": False},
        ]

        with patch.object(media_automation, "DRY_RUN", False), \
             patch.object(media_automation, "sonarr_get",
                          return_value=self._series_detail(all_seasons)), \
             patch.object(media_automation, "sonarr_put") as mock_put:
            changes = media_automation.apply_monitoring(
                series_id=1, title="Show", episodes=episodes,
                target_seasons={1}, all_seasons=all_seasons,
            )

        self.assertEqual(changes, 0)
        endpoints = [c.args[0] for c in mock_put.call_args_list]
        self.assertEqual(endpoints, ["/series/1"],
                         f"expected only the series PUT; got {endpoints}")


class SecretLogRedactionTests(unittest.TestCase):
    """Secrets must never reach `docker logs`. requests embeds the full request
    URL in its exception strings, so a naive `log.error(f"...: {e}")` leaks:
      - SABnzbd's ?apikey=<KEY> query param (media_automation.sabnzbd_api)
      - Discord/Slack's secret webhook token (trakt_discovery._send_alert_webhook)
    These guards simulate a ConnectionError carrying the secret-laden URL and
    assert the secret is absent from every captured log record."""

    SAB_KEY = "deadbeefcafe1234567890sabnzbdkey"
    WEBHOOK_TOKEN = "AbCdEf-SuperSecretDiscordToken-123456"

    def test_sabnzbd_api_key_not_logged_on_connection_error(self):
        """ConnectionError whose message includes ?apikey=<KEY> (exactly how
        requests surfaces a failed GET) must not leak the key into the log."""
        url = "http://sab.local:8080/api"
        leaky = requests.exceptions.ConnectionError(
            f"HTTPConnectionPool: failed connecting to "
            f"{url}?apikey={self.SAB_KEY}&mode=queue&output=json"
        )
        with patch.object(media_automation, "SABNZBD_URL", "http://sab.local:8080"), \
             patch.object(media_automation, "SABNZBD_API_KEY", self.SAB_KEY), \
             patch.object(media_automation.requests, "get", side_effect=leaky):
            with self.assertLogs(media_automation.log, level="ERROR") as cm:
                result = media_automation.sabnzbd_api("queue")

        self.assertIsNone(result)
        joined = "\n".join(cm.output)
        self.assertNotIn(self.SAB_KEY, joined)
        self.assertNotIn("apikey=", joined)
        # Still useful for debugging: names the failure type and the mode.
        self.assertIn("ConnectionError", joined)
        self.assertIn("mode=queue", joined)

    def test_alert_webhook_token_not_logged_on_post_failure(self):
        """A failed alert POST must not leak the secret token embedded in
        ALERT_WEBHOOK_URL (Discord/Slack put a token in the path)."""
        webhook = f"https://discord.com/api/webhooks/123456789/{self.WEBHOOK_TOKEN}"
        leaky = requests.exceptions.ConnectionError(
            f"HTTPSConnectionPool: failed connecting to {webhook}"
        )
        with patch.object(trakt_discovery, "ALERT_WEBHOOK_URL", webhook), \
             patch.object(trakt_discovery, "_send_alert_email"), \
             patch.object(trakt_discovery.requests, "post", side_effect=leaky):
            with self.assertLogs(trakt_discovery.log, level="WARNING") as cm:
                trakt_discovery._send_alert_webhook("token refresh failed")

        joined = "\n".join(cm.output)
        self.assertNotIn(self.WEBHOOK_TOKEN, joined)
        self.assertNotIn(webhook, joined)
        # The failure is still reported, just without the secret.
        self.assertIn("Alert webhook failed", joined)
        self.assertIn("ConnectionError", joined)


if __name__ == "__main__":
    unittest.main()
