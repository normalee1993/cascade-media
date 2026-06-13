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
