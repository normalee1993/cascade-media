"""Regression tests for the bug classes that bit production in 2026-05.

Each test covers a specific incident class — comments name the dates so future
maintainers can trace which real-world failure each guard exists to prevent.
"""

import io
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import media_automation
import scheduler
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


class SchedulerWebhookHardeningTests(unittest.TestCase):
    """Item 4 / P2-6: webhook server robustness.

    A malformed or oversized Content-Length must return a clean 400 / 413 instead
    of an unhandled traceback (which previously crashed do_POST before the try),
    and the body read must be capped so a hostile client can't make us allocate
    gigabytes. These exercise the pure read_request_body helper plus do_POST end
    to end with mocked rfile/wfile so no live socket is needed.
    """

    # ---- pure helper: read_request_body ----

    def test_bad_content_length_raises(self):
        """Non-integer Content-Length → BadContentLength (caller maps to 400)."""
        headers = {"Content-Length": "not-a-number"}
        with self.assertRaises(scheduler.BadContentLength):
            scheduler.read_request_body(headers, io.BytesIO(b"abc"))

    def test_negative_content_length_raises(self):
        """A negative length is malformed/hostile → BadContentLength (400)."""
        headers = {"Content-Length": "-5"}
        with self.assertRaises(scheduler.BadContentLength):
            scheduler.read_request_body(headers, io.BytesIO(b"abc"))

    def test_oversized_content_length_raises_before_read(self):
        """Length past the cap → BodyTooLarge (413), and we must NOT read the body
        (no giant allocation). rfile.read is left untouched."""
        headers = {"Content-Length": str(scheduler.MAX_WEBHOOK_BODY + 1)}
        rfile = MagicMock()
        with self.assertRaises(scheduler.BodyTooLarge):
            scheduler.read_request_body(headers, rfile)
        rfile.read.assert_not_called()

    def test_missing_content_length_reads_nothing(self):
        """No header → treated as zero-length body, returns b'' without reading."""
        rfile = MagicMock()
        body = scheduler.read_request_body({}, rfile)
        self.assertEqual(body, b"")
        rfile.read.assert_not_called()

    def test_valid_content_length_reads_exact_bytes(self):
        """Happy path: reads exactly Content-Length bytes."""
        payload = b'{"eventType":"Test"}'
        headers = {"Content-Length": str(len(payload))}
        body = scheduler.read_request_body(headers, io.BytesIO(payload))
        self.assertEqual(body, payload)

    def test_exactly_at_cap_is_allowed(self):
        """Boundary: Content-Length == cap is allowed (only strictly larger is 413)."""
        headers = {"Content-Length": str(scheduler.MAX_WEBHOOK_BODY)}
        rfile = MagicMock()
        rfile.read.return_value = b"x"  # don't actually allocate a MiB
        scheduler.read_request_body(headers, rfile)
        rfile.read.assert_called_once_with(scheduler.MAX_WEBHOOK_BODY)

    # ---- do_POST behavior via a handler built without a real socket ----

    def _make_handler(self, headers, rfile):
        """Build a WebhookHandler without invoking BaseHTTPRequestHandler.__init__
        (which would try to service a real connection). We only need the do_POST
        path, so we attach the attributes it touches and capture _respond calls."""
        handler = scheduler.WebhookHandler.__new__(scheduler.WebhookHandler)
        handler.headers = headers
        handler.rfile = rfile
        handler.wfile = io.BytesIO()
        handler._responses = []
        handler._respond = lambda code, data: handler._responses.append((code, data))
        return handler

    def test_do_post_malformed_length_returns_400(self):
        handler = self._make_handler({"Content-Length": "garbage"}, io.BytesIO(b""))
        handler.do_POST()
        self.assertEqual(handler._responses[-1][0], 400)

    def test_do_post_oversized_returns_413(self):
        handler = self._make_handler(
            {"Content-Length": str(scheduler.MAX_WEBHOOK_BODY + 1)}, MagicMock()
        )
        handler.do_POST()
        self.assertEqual(handler._responses[-1][0], 413)

    def test_do_post_malformed_json_returns_500_not_crash(self):
        """Valid Content-Length but non-JSON body falls into the catch-all and
        returns 500 — must not raise out of the handler."""
        payload = b"this is not json{{{"
        handler = self._make_handler(
            {"Content-Length": str(len(payload))}, io.BytesIO(payload)
        )
        handler.do_POST()
        self.assertEqual(handler._responses[-1][0], 500)

    def test_do_post_test_event_returns_200(self):
        payload = b'{"eventType":"Test"}'
        handler = self._make_handler(
            {"Content-Length": str(len(payload))}, io.BytesIO(payload)
        )
        handler.do_POST()
        self.assertEqual(handler._responses[-1], (200, {"status": "ok"}))

    def test_server_is_threading_and_daemon_capable(self):
        """ThreadingHTTPServer so one stuck client can't block health checks; the
        handler carries a per-request timeout so a half-open socket can't pin a
        thread forever."""
        from http.server import ThreadingHTTPServer
        self.assertIs(scheduler.ThreadingHTTPServer, ThreadingHTTPServer)
        self.assertEqual(scheduler.WebhookHandler.timeout, scheduler.WEBHOOK_REQUEST_TIMEOUT)


class SchedulerChildShutdownTests(unittest.TestCase):
    """Item 4 / P2-8: graceful child shutdown.

    On container stop the SIGTERM handler must forward terminate() to the live
    child subprocess so it stops cleanly within stop_grace_period instead of being
    SIGKILLed mid-write (half-applied Sonarr monitoring). We use a real short-lived
    child (`python -c "import time; time.sleep(...)"`) to verify tracking +
    termination without mocking subprocess internals.
    """

    def setUp(self):
        # Ensure a clean tracking set for each test.
        with scheduler._children_lock:
            scheduler._live_children.clear()

    def test_run_tracked_returns_completed_process(self):
        """Normal completion: returns a process with returncode, untracked after."""
        proc = scheduler._run_tracked([sys.executable, "-c", "pass"], timeout=30)
        self.assertEqual(proc.returncode, 0)
        with scheduler._children_lock:
            self.assertEqual(len(scheduler._live_children), 0)

    def test_run_tracked_preserves_nonzero_returncode(self):
        proc = scheduler._run_tracked([sys.executable, "-c", "import sys; sys.exit(3)"], timeout=30)
        self.assertEqual(proc.returncode, 3)

    def test_run_tracked_timeout_raises_and_untracks(self):
        """Overrunning the timeout raises TimeoutExpired (matching subprocess.run)
        and the killed child is removed from the live set."""
        with self.assertRaises(subprocess.TimeoutExpired):
            scheduler._run_tracked([sys.executable, "-c", "import time; time.sleep(30)"], timeout=1)
        with scheduler._children_lock:
            self.assertEqual(len(scheduler._live_children), 0)

    def test_shutdown_signal_terminates_live_child(self):
        """The crux of P2-8: while a child is running in _run_tracked on a worker
        thread, invoking the SIGTERM handler must terminate() that child so the
        blocked .wait() returns promptly (non-zero, signalled) instead of waiting
        out the full timeout."""
        # Reset the module-level event so the handler's set() is observable here.
        scheduler.shutdown_event.clear()
        results = {}

        def worker():
            try:
                results["proc"] = scheduler._run_tracked(
                    [sys.executable, "-c", "import time; time.sleep(30)"], timeout=60
                )
            except BaseException as e:  # pragma: no cover - shouldn't happen
                results["error"] = e

        t = threading.Thread(target=worker)
        t.start()

        # Wait until the child is registered as live.
        deadline = time.time() + 5
        while time.time() < deadline:
            with scheduler._children_lock:
                if scheduler._live_children:
                    break
            time.sleep(0.02)
        with scheduler._children_lock:
            self.assertTrue(scheduler._live_children, "child was never registered as live")

        # Invoke the real signal handler (as the kernel would on SIGTERM).
        scheduler._handle_shutdown_signal(signal.SIGTERM, None)

        t.join(timeout=10)
        self.assertFalse(t.is_alive(), "worker did not finish — child was not terminated")
        self.assertTrue(scheduler.shutdown_event.is_set())
        self.assertNotIn("error", results)
        # Child was killed by signal → negative returncode (e.g. -SIGTERM).
        self.assertLess(results["proc"].returncode, 0)
        # And it's no longer tracked.
        with scheduler._children_lock:
            self.assertEqual(len(scheduler._live_children), 0)

        scheduler.shutdown_event.clear()  # leave global state clean for other tests


if __name__ == "__main__":
    unittest.main()
