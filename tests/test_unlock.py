"""Tests for the cascade-unlock fixes in check_user_progress + the shared
unlock_and_download_season helper.

Covers two production bugs:
  * Bug 1 (aired-count denominator): progress % must be watched / AIRED episodes
    (from Sonarr), not watched / downloaded-episodes-Jellyfin-can-see. A
    preview-only season with one downloaded+watched E01 must NOT read as 100%.
  * Bug 2 (watch-an-E01-unlocks-that-season): watching the preview E01 of a
    locked season unlocks THAT season (force_e02=True), independent of the
    next-season cascade.

Plus the shared helper refactor (used by next-season, watched-E01, and the
live-playback path) and a regression test that check_active_playback still
unlocks through the helper.

Pattern notes (mirrors tests/test_boost.py):
  * Real temp SQLite via init_db() so the unlocked_seasons schema is exercised.
  * Every external seam (Sonarr /series + /episode + put/post, Jellyfin /Users,
    SABnzbd) is mocked; no network and no real sleeps.
  * boost_season_priority is mocked in the cascade tests so the only thing under
    test is the unlock decision + helper plumbing.
"""

import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import media_automation


def _iso_days_ago(days):
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def _iso_days_ahead(days):
    dt = datetime.now(timezone.utc) + timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


def _sonarr_ep(ep_id, season, episode, aired_days_ago=10, monitored=False, has_file=False):
    """A Sonarr /episode row. aired_days_ago=None => unaired (future air date)."""
    if aired_days_ago is None:
        air = _iso_days_ahead(30)
    else:
        air = _iso_days_ago(aired_days_ago)
    return {
        "id": ep_id,
        "seasonNumber": season,
        "episodeNumber": episode,
        "airDateUtc": air,
        "monitored": monitored,
        "hasFile": has_file,
    }


def _jf_watched(series_id, series_name, season, episode):
    """A Jellyfin /Items watched-episode row (IsPlayed=true) the cascade consumes."""
    return {
        "SeriesId": series_id,
        "SeriesName": series_name,
        "ParentIndexNumber": season,
        "IndexNumber": episode,
    }


class _Base(unittest.TestCase):
    SERIES_ID = 7
    JF_SERIES_ID = "jf-fam"
    SERIES_NAME = "For All Mankind"

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "media_automation.db")
        self._db_patch = patch.object(media_automation, "DB_PATH", self._db_path)
        self._db_patch.start()
        self.conn = media_automation.init_db()

        self._cfg = [
            patch.object(media_automation, "JELLYFIN_USER_IDS", ["user-1"]),
            patch.object(media_automation, "DRY_RUN", False),
            patch.object(media_automation, "WATCH_THRESHOLD", 0.75),
            patch.object(media_automation, "SABNZBD_QUEUE_WAIT_SECONDS", 0),
        ]
        for p in self._cfg:
            p.start()

    def tearDown(self):
        for p in self._cfg:
            p.stop()
        self.conn.close()
        self._db_patch.stop()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # --- helpers ----------------------------------------------------------

    def _series_maps(self):
        s = {"id": self.SERIES_ID, "title": self.SERIES_NAME, "tvdbId": 999}
        return {self.SERIES_NAME.lower(): s}, {999: s}, [s]

    def _watched_jf(self, watched_rows):
        """jellyfin_get side_effect returning the given watched-episode rows."""
        def fake(path, *args, **kwargs):
            if path.startswith("/Users/") and path.endswith("/Items"):
                return {"Items": watched_rows}
            if path.startswith("/Users/"):
                return {"Name": "alice"}
            raise AssertionError(f"unexpected jellyfin_get path: {path}")
        return fake

    def _sonarr_episodes(self, episodes):
        """sonarr_get side_effect: /episode returns the given list."""
        def fake(path, *args, **kwargs):
            if path.startswith("/episode?seriesId="):
                return episodes
            raise AssertionError(f"unexpected sonarr_get path: {path}")
        return fake

    def _run_progress(self, watched_rows, episodes, sonarr_episodes_override=None):
        """Run check_user_progress with mocks; returns the boost mock for assertions."""
        by_title, by_tvdb, all_series = self._series_maps()
        sonarr_side = (sonarr_episodes_override
                       if sonarr_episodes_override is not None
                       else self._sonarr_episodes(episodes))
        with patch.object(media_automation, "jellyfin_get",
                          side_effect=self._watched_jf(watched_rows)), \
             patch.object(media_automation, "sonarr_get", side_effect=sonarr_side), \
             patch.object(media_automation, "sonarr_put") as put, \
             patch.object(media_automation, "sonarr_post") as post, \
             patch.object(media_automation, "boost_season_priority") as boost:
            media_automation.check_user_progress(
                self.conn, "user-1", by_title, by_tvdb, all_series)
        return put, post, boost


class Bug1AiredDenominatorTests(_Base):
    """progress % uses AIRED Sonarr episodes, not downloaded-in-Jellyfin counts."""

    def test_preview_season_one_watched_of_ten_aired_does_not_unlock_next(self):
        # S04: only E01 downloaded + watched, but Sonarr says 10 episodes aired.
        # 1/10 = 10% < 75% -> Season 5 must NOT unlock (the real FAM bug).
        episodes = [_sonarr_ep(400 + i, 4, i) for i in range(1, 11)]
        episodes += [_sonarr_ep(500 + i, 5, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]
        put, post, boost = self._run_progress(watched, episodes)

        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 5))
        # No SeasonSearch for S05.
        for c in post.call_args_list:
            self.assertNotEqual(c.args[1].get("seasonNumber"), 5)

    def test_eighty_percent_of_aired_unlocks_next(self):
        # S01: 8 of 10 aired watched = 80% >= 75% -> Season 2 unlocks.
        episodes = [_sonarr_ep(100 + i, 1, i) for i in range(1, 11)]
        episodes += [_sonarr_ep(200 + i, 2, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 1, i) for i in range(1, 9)]
        put, post, boost = self._run_progress(watched, episodes)

        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 2))
        boost.assert_any_call(self.conn, self.SERIES_ID, 2, self.SERIES_NAME,
                              force_e02=False, session=None, stop_event=None)

    def test_unaired_episodes_excluded_from_denominator(self):
        # S01 has 10 episodes but only 5 have aired; 4 watched = 4/5 = 80% -> unlock.
        episodes = [_sonarr_ep(100 + i, 1, i, aired_days_ago=10) for i in range(1, 6)]
        episodes += [_sonarr_ep(100 + i, 1, i, aired_days_ago=None) for i in range(6, 11)]
        episodes += [_sonarr_ep(200 + i, 2, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 1, i) for i in range(1, 5)]
        put, post, boost = self._run_progress(watched, episodes)
        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 2))

    def test_zero_aired_episodes_skips_season(self):
        # All of S04 is unaired; watched E01 anyway. aired_count==0 -> skip next.
        # (Bug 2 will still unlock S04 itself; assert only that S05 stays locked.)
        episodes = [_sonarr_ep(400 + i, 4, i, aired_days_ago=None) for i in range(1, 11)]
        episodes += [_sonarr_ep(500 + i, 5, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]
        # Pre-unlock S04 so Bug-2 doesn't fire and muddy the assertion.
        media_automation.mark_season_unlocked(self.conn, self.SERIES_ID, 4, "tester")
        put, post, boost = self._run_progress(watched, episodes)
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 5))
        boost.assert_not_called()

    def test_sonarr_episode_list_not_a_list_skips_all_unlocks(self):
        # sonarr_get returns None (decode error) -> no fallback to JF count,
        # no unlocks at all this cycle.
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]

        def sonarr_none(path, *args, **kwargs):
            return None

        put, post, boost = self._run_progress(watched, [], sonarr_episodes_override=sonarr_none)
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 5))
        put.assert_not_called()
        post.assert_not_called()
        boost.assert_not_called()


class Bug2WatchE01UnlocksThatSeasonTests(_Base):
    """Watching the preview E01 of a locked season unlocks THAT season."""

    def test_preview_e01_watched_unlocks_current_season(self):
        episodes = [_sonarr_ep(400 + i, 4, i) for i in range(1, 11)]
        episodes += [_sonarr_ep(500 + i, 5, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]
        put, post, boost = self._run_progress(watched, episodes)

        # S04 unlocked (helper: monitor + SeasonSearch + mark + boost force_e02).
        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        # SeasonSearch was posted for S04.
        s04_search = [c for c in post.call_args_list
                      if c.args[1].get("seasonNumber") == 4
                      and c.args[1].get("name") == "SeasonSearch"]
        self.assertEqual(len(s04_search), 1)
        # Unmonitored S04 episodes were monitored (10 puts).
        self.assertEqual(put.call_count, 10)
        # Boosted S04 with force_e02=True.
        boost.assert_any_call(self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                              force_e02=True, session=None, stop_event=None)
        # S05 stays locked (1/10 = 10%).
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 5))

    def test_already_unlocked_season_not_reunlocked(self):
        episodes = [_sonarr_ep(400 + i, 4, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]
        media_automation.mark_season_unlocked(self.conn, self.SERIES_ID, 4, "tester")
        put, post, boost = self._run_progress(watched, episodes)
        # No new SeasonSearch / boost for S04.
        post.assert_not_called()
        boost.assert_not_called()

    def test_combined_fam_scenario(self):
        """Watch S04E01: S04 (10 aired, locked) unlocks; S05 (locked) does NOT."""
        episodes = [_sonarr_ep(400 + i, 4, i) for i in range(1, 11)]
        episodes += [_sonarr_ep(500 + i, 5, i) for i in range(1, 11)]
        watched = [_jf_watched(self.JF_SERIES_ID, self.SERIES_NAME, 4, 1)]
        put, post, boost = self._run_progress(watched, episodes)
        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 5))


class UnlockAndDownloadSeasonHelperTests(_Base):
    """The shared helper: monitoring, posting, marking, boosting, DRY_RUN."""

    def _episodes(self):
        return [
            _sonarr_ep(1, 4, 1, monitored=True),    # already monitored -> no put
            _sonarr_ep(2, 4, 2, monitored=False),   # -> put
            _sonarr_ep(3, 4, 3, monitored=False),   # -> put
            _sonarr_ep(99, 5, 1, monitored=False),  # other season -> ignored
        ]

    def test_monitors_only_unmonitored_episodes(self):
        eps = self._episodes()
        with patch.object(media_automation, "sonarr_put") as put, \
             patch.object(media_automation, "sonarr_post") as post, \
             patch.object(media_automation, "boost_season_priority") as boost:
            result = media_automation.unlock_and_download_season(
                self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                "tester", eps, force_e02=True)

        self.assertTrue(result)
        # Only the two unmonitored S04 episodes were PUT.
        self.assertEqual(put.call_count, 2)
        put_ids = {c.args[0] for c in put.call_args_list}
        self.assertEqual(put_ids, {"/episode/2", "/episode/3"})
        # One SeasonSearch for S04.
        post.assert_called_once()
        self.assertEqual(post.call_args.args[1]["seasonNumber"], 4)
        # Marked unlocked + boosted with force_e02 propagated.
        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        boost.assert_called_once_with(self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                                      force_e02=True, session=None, stop_event=None)

    def test_no_episodes_for_season_returns_false(self):
        eps = [_sonarr_ep(99, 5, 1)]  # nothing for S04
        with patch.object(media_automation, "sonarr_put") as put, \
             patch.object(media_automation, "sonarr_post") as post, \
             patch.object(media_automation, "boost_season_priority") as boost:
            result = media_automation.unlock_and_download_season(
                self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                "tester", eps, force_e02=False)
        self.assertFalse(result)
        put.assert_not_called()
        post.assert_not_called()
        boost.assert_not_called()
        self.assertFalse(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))

    def test_dry_run_makes_no_writes_or_posts(self):
        eps = self._episodes()
        with patch.object(media_automation, "DRY_RUN", True), \
             patch.object(media_automation, "sonarr_put") as put, \
             patch.object(media_automation, "sonarr_post") as post, \
             patch.object(media_automation, "boost_season_priority") as boost:
            media_automation.unlock_and_download_season(
                self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                "tester", eps, force_e02=False)
        put.assert_not_called()
        post.assert_not_called()
        # Still marks unlocked + calls boost (boost itself honors DRY_RUN).
        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        boost.assert_called_once()


class CheckActivePlaybackRegressionTests(_Base):
    """check_active_playback still unlocks via the shared helper."""

    def test_playback_e01_unlocks_full_season(self):
        episodes = [_sonarr_ep(400 + i, 4, i, monitored=False) for i in range(1, 11)]

        def sonarr_get(path, *args, **kwargs):
            if path == "/series":
                return [{"id": self.SERIES_ID, "title": self.SERIES_NAME, "tvdbId": 999}]
            if path.startswith("/episode?seriesId="):
                return episodes
            raise AssertionError(f"unexpected sonarr_get path: {path}")

        sessions = [{
            "UserId": "user-1",
            "UserName": "alice",
            "NowPlayingItem": {
                "Type": "Episode",
                "IndexNumber": 1,
                "ParentIndexNumber": 4,
                "SeriesName": self.SERIES_NAME,
                "SeriesId": self.JF_SERIES_ID,
            },
        }]

        def jellyfin_get(path, *args, **kwargs):
            if path == "/Sessions":
                return sessions
            raise AssertionError(f"unexpected jellyfin_get path: {path}")

        with patch.object(media_automation, "sonarr_get", side_effect=sonarr_get), \
             patch.object(media_automation, "jellyfin_get", side_effect=jellyfin_get), \
             patch.object(media_automation, "sonarr_put") as put, \
             patch.object(media_automation, "sonarr_post") as post, \
             patch.object(media_automation, "boost_season_priority") as boost:
            media_automation.check_active_playback(self.conn)

        self.assertTrue(media_automation.is_season_unlocked(self.conn, self.SERIES_ID, 4))
        self.assertEqual(put.call_count, 10)
        post.assert_called_once()
        self.assertEqual(post.call_args.args[1]["seasonNumber"], 4)
        # force_e02=True preserved for the playback path.
        boost.assert_called_once_with(self.conn, self.SERIES_ID, 4, self.SERIES_NAME,
                                      force_e02=True, session=None, stop_event=None)


if __name__ == "__main__":
    unittest.main()
