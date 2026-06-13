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
class AtomicClaimTests(unittest.TestCase):
    """Item 2 (2026-06-13): atomic + durable processed_series claim.

    Root causes fixed:
      P1-2 durability — the old code committed 'processed' at the START of
        process_new_series, before apply_monitoring/searches ran. A transient
        Sonarr error afterwards left the series permanently flagged processed
        with its monitoring never applied, so Sonarr kept every season monitored
        and grabbed the whole library, never retried.
      P1-3 cross-process race — the poll subprocess and a SeriesAdd webhook
        subprocess (separate OS processes, no shared lock) could process the same
        newly-added series concurrently → duplicate monitor flips, duplicate
        SeasonSearch, duplicate SABnzbd grabs.

    These use a REAL temp SQLite file through init_db() so the schema/migration
    and the INSERT ... ON CONFLICT claim are exercised exactly as in production
    (WAL, separate connections). Sonarr/Seerr HTTP is mocked.
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "media_automation.db")
        self._db_patch = patch.object(media_automation, "DB_PATH", self._db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _new_conn(self):
        """A fresh connection via init_db — mimics a separate subprocess."""
        return media_automation.init_db()

    def _row(self, conn, sonarr_id):
        c = conn.cursor()
        conn.commit()
        c.execute("SELECT status, processed_at FROM processed_series WHERE sonarr_id = ?", (sonarr_id,))
        return c.fetchone()

    # ---- schema / migration safety ---------------------------------------

    def test_fresh_install_has_status_column(self):
        """A brand-new DB created by init_db has the status column."""
        conn = self._new_conn()
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(processed_series)")}
            self.assertIn("status", cols)
        finally:
            conn.close()

    def test_migration_on_populated_legacy_db_backfills_done(self):
        """An EXISTING DB built with the OLD schema (no status column) and
        populated rows must migrate cleanly: the column is added and legacy rows
        — which represent already-finished series — are treated as 'done'.
        Re-running init_db must be idempotent (no 'duplicate column' error)."""
        # Build a legacy DB by hand (old schema, no status column).
        legacy = sqlite3.connect(self._db_path)
        legacy.execute("""
            CREATE TABLE processed_series (
                sonarr_id INTEGER PRIMARY KEY,
                title TEXT,
                processed_at TEXT
            )
        """)
        legacy.execute(
            "INSERT INTO processed_series (sonarr_id, title, processed_at) VALUES (?, ?, ?)",
            (42, "Legacy Show", "2026-01-01T00:00:00+00:00"),
        )
        legacy.commit()
        legacy.close()

        # First init_db migrates; second proves idempotency.
        conn = media_automation.init_db()
        conn.close()
        conn = media_automation.init_db()
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(processed_series)")}
            self.assertIn("status", cols)
            status = self._row(conn, 42)[0]
            self.assertEqual(status, "done", "legacy rows must backfill to 'done'")
            # A legacy 'done' row is reported processed and is NOT reclaimable.
            self.assertTrue(media_automation.is_series_processed(conn, 42))
            self.assertFalse(media_automation.claim_series_for_processing(conn, 42, "Legacy Show"))
        finally:
            conn.close()

    # ---- atomic claim semantics ------------------------------------------

    def test_first_claim_wins_second_concurrent_claim_loses(self):
        """The claim is atomic: the first claimer wins (row created, 'in_progress')
        and a SECOND claim attempt for the same id while still 'in_progress'
        (non-stale) loses — it must NOT redo setup. Models poll vs webhook."""
        conn_a = self._new_conn()
        conn_b = self._new_conn()
        try:
            won_a = media_automation.claim_series_for_processing(conn_a, 7, "Race Show")
            won_b = media_automation.claim_series_for_processing(conn_b, 7, "Race Show")
            self.assertTrue(won_a, "first claimer must win")
            self.assertFalse(won_b, "second concurrent claimer must lose (no duplicate setup)")
            self.assertEqual(self._row(conn_a, 7)[0], "in_progress")
        finally:
            conn_a.close()
            conn_b.close()

    def test_done_series_is_not_reclaimable(self):
        """Once status='done', a later claim attempt returns False (skip)."""
        conn = self._new_conn()
        try:
            self.assertTrue(media_automation.claim_series_for_processing(conn, 9, "Done Show"))
            media_automation.mark_series_done(conn, 9, "Done Show")
            self.assertTrue(media_automation.is_series_processed(conn, 9))
            self.assertFalse(media_automation.claim_series_for_processing(conn, 9, "Done Show"))
        finally:
            conn.close()

    def test_stale_in_progress_is_reclaimable(self):
        """An 'in_progress' claim older than STALE_CLAIM_MINUTES self-heals: a
        later run can re-claim it (crash-mid-setup recovery)."""
        conn = self._new_conn()
        try:
            # Forge a stale claim directly.
            stale_ts = (media_automation.datetime.now(media_automation.timezone.utc)
                        - media_automation.timedelta(minutes=media_automation.STALE_CLAIM_MINUTES + 5)).isoformat()
            with conn:
                conn.execute(
                    "INSERT INTO processed_series (sonarr_id, title, status, processed_at) VALUES (?, ?, 'in_progress', ?)",
                    (11, "Crashed Show", stale_ts),
                )
            # is_series_processed must NOT report a stale in_progress as processed.
            self.assertFalse(media_automation.is_series_processed(conn, 11))
            self.assertTrue(
                media_automation.claim_series_for_processing(conn, 11, "Crashed Show"),
                "stale in_progress claim must be reclaimable",
            )
            # Reclaim refreshes the timestamp so a fresh (non-stale) claim is held.
            row = self._row(conn, 11)
            self.assertEqual(row[0], "in_progress")
            self.assertFalse(media_automation._claim_is_stale(row[1], media_automation.datetime.now(media_automation.timezone.utc)))
        finally:
            conn.close()

    def test_fresh_in_progress_not_reclaimable(self):
        """A non-stale 'in_progress' claim is NOT reclaimable (guards the race)."""
        conn = self._new_conn()
        try:
            self.assertTrue(media_automation.claim_series_for_processing(conn, 13, "Busy Show"))
            self.assertFalse(media_automation.claim_series_for_processing(conn, 13, "Busy Show"))
        finally:
            conn.close()

    # ---- durability: failed setup leaves series re-claimable -------------

    def _series(self, sonarr_id=21, title="Durability Show"):
        return {"id": sonarr_id, "title": title, "tvdbId": 555}

    def _episodes(self):
        return [
            {"id": 1, "seasonNumber": 1, "episodeNumber": 1, "monitored": True, "hasFile": False},
            {"id": 2, "seasonNumber": 1, "episodeNumber": 2, "monitored": True, "hasFile": False},
            {"id": 3, "seasonNumber": 2, "episodeNumber": 1, "monitored": True, "hasFile": False},
        ]

    def test_failed_apply_monitoring_leaves_series_reclaimable_and_retries(self):
        """P1-2 durability fix: if apply_monitoring raises, the claim is released
        (not flagged 'done'), so the series is re-claimable and the NEXT run
        retries it instead of locking it half-configured forever."""
        conn = self._new_conn()
        try:
            series = self._series()
            with patch.object(media_automation, "DRY_RUN", True), \
                 patch.object(media_automation, "sonarr_get", return_value=self._episodes()), \
                 patch.object(media_automation, "get_requested_seasons_from_seerr", return_value={1}), \
                 patch.object(media_automation, "apply_monitoring",
                              side_effect=RuntimeError("transient Sonarr 500")):
                with self.assertRaises(RuntimeError):
                    media_automation.process_new_series(conn, series)

            # Must NOT be marked done; row should be gone (released) → reclaimable.
            self.assertFalse(media_automation.is_series_processed(conn, series["id"]))
            self.assertIsNone(self._row(conn, series["id"]),
                              "failed setup must release the claim row entirely")

            # Next run succeeds → ends 'done', and apply_monitoring/searches ran.
            with patch.object(media_automation, "DRY_RUN", True), \
                 patch.object(media_automation, "sonarr_get", return_value=self._episodes()), \
                 patch.object(media_automation, "get_requested_seasons_from_seerr", return_value={1}), \
                 patch.object(media_automation, "apply_monitoring") as good_apply:
                media_automation.process_new_series(conn, series)
                good_apply.assert_called()
            self.assertTrue(media_automation.is_series_processed(conn, series["id"]))
            self.assertEqual(self._row(conn, series["id"])[0], "done")
        finally:
            conn.close()

    def test_successful_process_new_series_flips_to_done_only_at_end(self):
        """Happy path: status is 'done' only AFTER setup completes."""
        conn = self._new_conn()
        try:
            series = self._series(sonarr_id=23, title="Happy Show")
            with patch.object(media_automation, "DRY_RUN", True), \
                 patch.object(media_automation, "sonarr_get", return_value=self._episodes()), \
                 patch.object(media_automation, "get_requested_seasons_from_seerr", return_value={1}), \
                 patch.object(media_automation, "apply_monitoring", return_value=0):
                media_automation.process_new_series(conn, series)
            self.assertEqual(self._row(conn, 23)[0], "done")
            # Target season was unlocked as part of successful setup.
            self.assertTrue(media_automation.is_season_unlocked(conn, 23, 1))
        finally:
            conn.close()

    def test_second_process_skips_when_already_in_progress(self):
        """If a series is already claimed 'in_progress' (non-stale), a second
        process_new_series call short-circuits before touching apply_monitoring,
        so no duplicate monitor flips / searches occur (P1-3)."""
        conn = self._new_conn()
        try:
            series = self._series(sonarr_id=25, title="Inflight Show")
            # Pre-existing fresh in_progress claim by a "different process".
            media_automation.claim_series_for_processing(conn, 25, "Inflight Show")
            with patch.object(media_automation, "DRY_RUN", True), \
                 patch.object(media_automation, "sonarr_get", return_value=self._episodes()), \
                 patch.object(media_automation, "get_requested_seasons_from_seerr", return_value={1}), \
                 patch.object(media_automation, "apply_monitoring") as apply_mock:
                media_automation.process_new_series(conn, series)
                apply_mock.assert_not_called()
            # Still in_progress (owned by the original claimant), not flipped done.
            self.assertEqual(self._row(conn, 25)[0], "in_progress")
        finally:
            conn.close()
class ApiNoneResponseContractTests(unittest.TestCase):
    """P2-1: _api_request_with_retry returns None for a JSON-decode failure /
    no usable body. Callers iterate the result as a list/dict, so an unguarded
    None throws 'NoneType is not iterable' and aborts the whole cycle. Each
    iterating call site must log + skip instead of crashing."""

    def test_set_initial_monitoring_skips_on_none_series(self):
        with patch.object(media_automation, "sonarr_get", return_value=None):
            # Must not raise; with no series list there is nothing to process.
            media_automation.set_initial_monitoring(conn=MagicMock())

    def test_check_watch_progress_skips_on_none_series(self):
        with patch.object(media_automation, "sonarr_get", return_value=None), \
             patch.object(media_automation, "check_user_progress") as mock_progress:
            media_automation.check_watch_progress(conn=MagicMock())
        # Bailed before the per-user loop — no progress checks attempted.
        mock_progress.assert_not_called()

    def test_process_existing_series_skips_on_none_series(self):
        with patch.object(media_automation, "sonarr_get", return_value=None):
            media_automation.process_existing_series(conn=MagicMock())

    def test_check_active_playback_skips_on_none_series(self):
        # Sessions returns a playing episode; the Sonarr /series fetch returns
        # None. The handler must bail cleanly rather than iterate None.
        def fake_get(endpoint, *a, **k):
            if endpoint == "/Sessions":
                return [{"NowPlayingItem": {"Type": "Episode", "IndexNumber": 1,
                                            "ParentIndexNumber": 1, "SeriesName": "X"},
                         "UserId": "u1"}]
            return None  # /series -> None
        with patch.object(media_automation, "jellyfin_get", side_effect=fake_get), \
             patch.object(media_automation, "sonarr_get", return_value=None), \
             patch.object(media_automation, "JELLYFIN_USER_IDS", ["u1"]):
            media_automation.check_active_playback(conn=MagicMock())

    def test_cleanup_unwanted_queue_items_skips_on_none_episodes(self):
        with patch.object(media_automation, "sonarr_get", return_value=None):
            cancelled = media_automation.cleanup_unwanted_queue_items(1, "Show")
        self.assertEqual(cancelled, 0)

    def test_boost_season_priority_skips_on_none_episodes(self):
        # Past the configured/boosted guards: SABnzbd key set, season not yet
        # boosted, then the episode fetch returns None.
        conn = MagicMock()
        with patch.object(media_automation, "SABNZBD_API_KEY", "key"), \
             patch.object(media_automation, "is_season_boosted", return_value=False), \
             patch.object(media_automation, "sonarr_get", return_value=None), \
             patch.object(media_automation, "sabnzbd_get_queue") as mock_sab:
            media_automation.boost_season_priority(conn, 1, 2, "Show")
        # Bailed before touching the SABnzbd queue.
        mock_sab.assert_not_called()

    def test_apply_monitoring_skips_when_series_detail_none(self):
        """The season-level gate needs the series body; a None detail must abort
        the update (return 0) rather than PUT a None body or skip the gate."""
        episodes = [
            {"id": 1, "seasonNumber": 1, "episodeNumber": 1, "monitored": False, "hasFile": False},
        ]
        with patch.object(media_automation, "DRY_RUN", False), \
             patch.object(media_automation, "sonarr_get", return_value=None), \
             patch.object(media_automation, "sonarr_put") as mock_put:
            changes = media_automation.apply_monitoring(
                series_id=1, title="Show", episodes=episodes,
                target_seasons={1}, all_seasons={1},
            )
        self.assertEqual(changes, 0)
        mock_put.assert_not_called()


class CleanupStaleDbGuardTests(unittest.TestCase):
    """cleanup_stale_db_entries DELETEs every row whose series isn't in the
    Sonarr snapshot. A partial/empty/None response would wipe processed/unlock/
    boost state and trigger mass reprocessing. The plausibility guard must skip
    cleanup entirely on a suspect snapshot — issuing zero DELETEs."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "stale.db")
        conn = sqlite3.connect(self._db_path)
        conn.executescript(
            """
            CREATE TABLE processed_series (sonarr_id INTEGER PRIMARY KEY, title TEXT, processed_at TEXT);
            CREATE TABLE unlocked_seasons (sonarr_id INTEGER, season_number INTEGER, unlocked_by TEXT, unlocked_at TEXT);
            CREATE TABLE priority_boosts (sonarr_id INTEGER, season_number INTEGER, boosted_at TEXT);
            """
        )
        # Seed three processed series.
        for sid in (10, 20, 30):
            conn.execute("INSERT INTO processed_series VALUES (?, ?, ?)", (sid, f"Show{sid}", "2026-01-01"))
            conn.execute("INSERT INTO unlocked_seasons VALUES (?, ?, ?, ?)", (sid, 1, "initial", "2026-01-01"))
            conn.execute("INSERT INTO priority_boosts VALUES (?, ?, ?)", (sid, 1, "2026-01-01"))
        conn.commit()
        conn.close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _row_count(self):
        conn = sqlite3.connect(self._db_path)
        try:
            return conn.execute("SELECT COUNT(*) FROM processed_series").fetchone()[0]
        finally:
            conn.close()

    def test_skips_on_none_series(self):
        conn = sqlite3.connect(self._db_path)
        try:
            with patch.object(media_automation, "sonarr_get", return_value=None):
                media_automation.cleanup_stale_db_entries(conn)
        finally:
            conn.close()
        self.assertEqual(self._row_count(), 3, "None snapshot must not delete any rows")

    def test_skips_on_empty_series(self):
        conn = sqlite3.connect(self._db_path)
        try:
            with patch.object(media_automation, "sonarr_get", return_value=[]):
                media_automation.cleanup_stale_db_entries(conn)
        finally:
            conn.close()
        self.assertEqual(self._row_count(), 3, "Empty snapshot must not wipe the DB")

    def test_skips_on_zero_overlap_truncated_snapshot(self):
        # Snapshot is non-empty but shares NO ids with the DB -> looks truncated.
        conn = sqlite3.connect(self._db_path)
        try:
            with patch.object(media_automation, "sonarr_get",
                              return_value=[{"id": 999}]):
                media_automation.cleanup_stale_db_entries(conn)
        finally:
            conn.close()
        self.assertEqual(self._row_count(), 3, "Zero-overlap snapshot must not mass-delete")

    def test_deletes_only_genuinely_stale_rows_on_valid_snapshot(self):
        # Snapshot overlaps (10, 20 present) and drops 30 -> 30 is genuinely stale.
        conn = sqlite3.connect(self._db_path)
        try:
            with patch.object(media_automation, "sonarr_get",
                              return_value=[{"id": 10}, {"id": 20}]):
                media_automation.cleanup_stale_db_entries(conn)
        finally:
            conn.close()
        conn = sqlite3.connect(self._db_path)
        try:
            remaining = {r[0] for r in conn.execute("SELECT sonarr_id FROM processed_series")}
        finally:
            conn.close()
        self.assertEqual(remaining, {10, 20}, "Only the genuinely absent series should be removed")


class MediaAutomationDbWritabilityProbeTests(unittest.TestCase):
    """P2-4: media_automation had no readonly-DB probe (unlike trakt_discovery),
    so a root-owned bind-mount DB crashed init_db every cycle with a cryptic
    error. check_db_writable mirrors trakt's probe (PRAGMA user_version) and is
    called at the start of every run_* entrypoint."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "probe.db")
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
            self.assertTrue(media_automation.check_db_writable(conn))
        finally:
            conn.close()

    def test_readonly_db_fails_and_logs(self):
        """Open via URI in mode=ro. Probe must return False and log an actionable
        message naming the chown fix."""
        conn = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            with self.assertLogs(media_automation.log, level="ERROR") as cm:
                self.assertFalse(media_automation.check_db_writable(conn))
            self.assertTrue(any("chown 99:users" in line for line in cm.output),
                            "readonly message must name the chown fix")
        finally:
            conn.close()


class RetryAfterParseTests(unittest.TestCase):
    """P2-12: int(resp.headers.get('Retry-After', 60)) crashes on an HTTP-date
    value (RFC 7231 allows date OR seconds). A date-form header must fall back to
    the 60s default instead of aborting the cycle. Covers both modules."""

    @staticmethod
    def _resp(status, headers):
        resp = MagicMock()
        resp.status_code = status
        resp.headers = headers
        resp.json.return_value = {"ok": True}
        resp.raise_for_status.return_value = None
        return resp

    def test_media_automation_date_form_retry_after_does_not_raise(self):
        # First call: 429 with an HTTP-date Retry-After. Second: success.
        responses = [
            self._resp(429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
            self._resp(200, {}),
        ]
        method = MagicMock(side_effect=responses)
        with patch.object(media_automation.time, "sleep") as mock_sleep:
            result = media_automation._api_request_with_retry(method, "http://x", {})
        self.assertEqual(result, {"ok": True})
        # Fell back to the 60s default rather than raising.
        mock_sleep.assert_called_once_with(60)

    def test_media_automation_numeric_retry_after_is_honored(self):
        responses = [
            self._resp(429, {"Retry-After": "5"}),
            self._resp(200, {}),
        ]
        method = MagicMock(side_effect=responses)
        with patch.object(media_automation.time, "sleep") as mock_sleep:
            media_automation._api_request_with_retry(method, "http://x", {})
        mock_sleep.assert_called_once_with(5)

    def test_trakt_date_form_retry_after_does_not_raise(self):
        responses = [
            self._resp(429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
            self._resp(200, {}),
        ]
        method = MagicMock(side_effect=responses)
        with patch.object(trakt_discovery.time, "sleep") as mock_sleep:
            result = trakt_discovery._api_request_with_retry(method, "http://x", {})
        self.assertEqual(result, {"ok": True})
        mock_sleep.assert_called_once_with(60)


if __name__ == "__main__":
    unittest.main()
