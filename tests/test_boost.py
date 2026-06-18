"""Tests for the Phase 1 in-progress weekly priority boost (boost_in_progress_episodes).

These exercise the new per-episode boost ledger (episode_boosts) and the
in-progress sweep that bumps still-queued episodes of an actively-watched,
already-unlocked season to High priority in SABnzbd.

Pattern notes:
  * episode_boosts ledger tests use a REAL temp SQLite file through init_db()
    so the new table + PRIMARY KEY constraint are exercised as in production
    (WAL, idempotent INSERT OR REPLACE).
  * The sweep tests mock every external seam (Sonarr /series + /episode +
    /queue, Jellyfin /Users/.../Items, SABnzbd queue + set_priority) so no
    network is touched, and assert on which set_priority calls and ledger
    writes happened.
"""

import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import media_automation


def _jf_episode(series_id, series_name, season, episode, played_at):
    """A Jellyfin /Items episode row in the shape the sweep consumes."""
    return {
        "SeriesId": series_id,
        "SeriesName": series_name,
        "ParentIndexNumber": season,
        "IndexNumber": episode,
        "UserData": {"LastPlayedDate": played_at},
    }


def _iso_days_ago(days):
    """An ISO-8601 UTC timestamp `days` in the past, Jellyfin 'Z' style."""
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")


class EpisodeBoostLedgerTests(unittest.TestCase):
    """is_episode_boosted / mark_episode_boosted against a real init_db schema."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "media_automation.db")
        self._db_patch = patch.object(media_automation, "DB_PATH", self._db_path)
        self._db_patch.start()
        self.conn = media_automation.init_db()

    def tearDown(self):
        self.conn.close()
        self._db_patch.stop()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_table_created_with_episode_pk(self):
        cols = {r[1] for r in self.conn.execute("PRAGMA table_info(episode_boosts)")}
        self.assertEqual(cols, {"sonarr_id", "season_number", "episode_number", "boosted_at"})

    def test_set_then_query(self):
        self.assertFalse(media_automation.is_episode_boosted(self.conn, 10, 2, 5))
        media_automation.mark_episode_boosted(self.conn, 10, 2, 5)
        self.assertTrue(media_automation.is_episode_boosted(self.conn, 10, 2, 5))
        # Different episode in the same season is unaffected.
        self.assertFalse(media_automation.is_episode_boosted(self.conn, 10, 2, 6))

    def test_mark_is_idempotent(self):
        media_automation.mark_episode_boosted(self.conn, 10, 2, 5)
        # A second mark must not raise (INSERT OR REPLACE) and stays a single row.
        media_automation.mark_episode_boosted(self.conn, 10, 2, 5)
        rows = self.conn.execute(
            "SELECT COUNT(*) FROM episode_boosts WHERE sonarr_id=? AND season_number=? AND episode_number=?",
            (10, 2, 5)).fetchone()[0]
        self.assertEqual(rows, 1)

    def test_does_not_touch_season_priority_boosts(self):
        """The per-episode ledger is distinct from the season-level table."""
        media_automation.mark_episode_boosted(self.conn, 10, 2, 5)
        self.assertFalse(media_automation.is_season_boosted(self.conn, 10, 2))


class InProgressBoostSweepTests(unittest.TestCase):
    """boost_in_progress_episodes end-to-end with mocked Sonarr/Jellyfin/SABnzbd."""

    SERIES_ID = 42
    JF_SERIES_ID = "jf-abc"
    SEASON = 1

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db_path = os.path.join(self._tmpdir, "media_automation.db")
        self._db_patch = patch.object(media_automation, "DB_PATH", self._db_path)
        self._db_patch.start()
        self.conn = media_automation.init_db()
        # Current season is unlocked/in-progress; next season NOT yet unlocked.
        media_automation.mark_season_unlocked(self.conn, self.SERIES_ID, self.SEASON, "tester")

        # Common config: SABnzbd configured, one user, not a dry run, 7-day window.
        self._cfg = [
            patch.object(media_automation, "SABNZBD_API_KEY", "sab-key"),
            patch.object(media_automation, "JELLYFIN_USER_IDS", ["user-1"]),
            patch.object(media_automation, "DRY_RUN", False),
            patch.object(media_automation, "INPROGRESS_BOOST_WINDOW_DAYS", 7),
        ]
        for p in self._cfg:
            p.start()

    def tearDown(self):
        for p in self._cfg:
            p.stop()
        self.conn.close()
        self._db_patch.stop()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    # ---- mock factories --------------------------------------------------

    def _sonarr_get(self, watched_days_ago=1, season=None):
        """Build a sonarr_get side_effect for /series, /episode, /queue."""
        season = self.SEASON if season is None else season

        def fake(path, *args, **kwargs):
            if path == "/series":
                return [{"id": self.SERIES_ID, "title": "Test Show",
                         "tvdbId": 999}]
            if path.startswith("/episode?seriesId="):
                # E01 (already watched) and E02 (still queued).
                return [
                    {"id": 1001, "seasonNumber": season, "episodeNumber": 1},
                    {"id": 1002, "seasonNumber": season, "episodeNumber": 2},
                ]
            if path.startswith("/queue"):
                return {"records": [
                    {"seriesId": self.SERIES_ID, "episodeId": 1002, "downloadId": "NZO-E02"},
                ]}
            raise AssertionError(f"unexpected sonarr_get path: {path}")

        return fake

    def _jellyfin_get(self, watched_days_ago=1, season=None):
        season = self.SEASON if season is None else season

        def fake(path, *args, **kwargs):
            if path.startswith("/Users/") and path.endswith("/Items"):
                return {"Items": [
                    _jf_episode(self.JF_SERIES_ID, "Test Show", season, 1,
                                _iso_days_ago(watched_days_ago)),
                ]}
            raise AssertionError(f"unexpected jellyfin_get path: {path}")

        return fake

    def _sab_queue(self):
        return [{"nzo_id": "NZO-E02"}]

    def _run(self, watched_days_ago=1, season=None):
        """Run the sweep with default happy-path mocks; returns the set_priority mock."""
        with patch.object(media_automation, "sonarr_get",
                          side_effect=self._sonarr_get(watched_days_ago, season)), \
             patch.object(media_automation, "jellyfin_get",
                          side_effect=self._jellyfin_get(watched_days_ago, season)), \
             patch.object(media_automation, "sabnzbd_get_queue",
                          return_value=self._sab_queue()), \
             patch.object(media_automation, "sabnzbd_set_priority",
                          return_value=True) as set_pri:
            media_automation.boost_in_progress_episodes(self.conn)
        return set_pri

    # ---- behavior --------------------------------------------------------

    def test_in_window_boosts_queued_episode(self):
        set_pri = self._run(watched_days_ago=1)
        set_pri.assert_called_once_with("NZO-E02", 1)  # priority 1 == High
        self.assertTrue(
            media_automation.is_episode_boosted(self.conn, self.SERIES_ID, self.SEASON, 2))

    def test_outside_window_does_not_boost(self):
        """A season last played 8 days ago is stale -> no boost, no ledger write."""
        set_pri = self._run(watched_days_ago=8)
        set_pri.assert_not_called()
        self.assertFalse(
            media_automation.is_episode_boosted(self.conn, self.SERIES_ID, self.SEASON, 2))

    def test_locked_season_skipped(self):
        """A played season that is NOT unlocked must be skipped entirely."""
        # Use a season with no unlocked_seasons row.
        set_pri = self._run(watched_days_ago=1, season=5)
        set_pri.assert_not_called()
        self.assertFalse(
            media_automation.is_episode_boosted(self.conn, self.SERIES_ID, 5, 2))

    def test_next_season_already_unlocked_skipped(self):
        """If the cascade already unlocked the next season, the in-progress boost
        steps aside (boost_season_priority owns that path)."""
        media_automation.mark_season_unlocked(self.conn, self.SERIES_ID, self.SEASON + 1, "tester")
        set_pri = self._run(watched_days_ago=1)
        set_pri.assert_not_called()

    def test_already_boosted_episode_skipped(self):
        media_automation.mark_episode_boosted(self.conn, self.SERIES_ID, self.SEASON, 2)
        set_pri = self._run(watched_days_ago=1)
        set_pri.assert_not_called()

    def test_dry_run_makes_no_calls_or_writes(self):
        with patch.object(media_automation, "DRY_RUN", True):
            set_pri = self._run(watched_days_ago=1)
        set_pri.assert_not_called()
        self.assertFalse(
            media_automation.is_episode_boosted(self.conn, self.SERIES_ID, self.SEASON, 2))

    def test_no_sabnzbd_key_noop(self):
        with patch.object(media_automation, "SABNZBD_API_KEY", ""), \
             patch.object(media_automation, "sonarr_get") as sg, \
             patch.object(media_automation, "jellyfin_get") as jg, \
             patch.object(media_automation, "sabnzbd_set_priority") as set_pri:
            media_automation.boost_in_progress_episodes(self.conn)
        sg.assert_not_called()
        jg.assert_not_called()
        set_pri.assert_not_called()

    def test_window_disabled_noop(self):
        with patch.object(media_automation, "INPROGRESS_BOOST_WINDOW_DAYS", 0), \
             patch.object(media_automation, "sonarr_get") as sg, \
             patch.object(media_automation, "sabnzbd_set_priority") as set_pri:
            media_automation.boost_in_progress_episodes(self.conn)
        sg.assert_not_called()
        set_pri.assert_not_called()

    def test_second_run_is_idempotent(self):
        """First run boosts E02; a second run finds it ledgered and does nothing."""
        first = self._run(watched_days_ago=1)
        first.assert_called_once()
        second = self._run(watched_days_ago=1)
        second.assert_not_called()


class ParseJfDatetimeTests(unittest.TestCase):
    """_parse_jf_datetime — robust to Jellyfin's 7-digit fractional 'Z' format."""

    def test_seven_digit_fractional_z(self):
        dt = media_automation._parse_jf_datetime("2026-06-15T20:11:33.0000000Z")
        self.assertIsNotNone(dt)
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual((dt.year, dt.month, dt.day), (2026, 6, 15))

    def test_plain_offset(self):
        dt = media_automation._parse_jf_datetime("2026-06-15T20:11:33+00:00")
        self.assertIsNotNone(dt)

    def test_none_and_empty(self):
        self.assertIsNone(media_automation._parse_jf_datetime(None))
        self.assertIsNone(media_automation._parse_jf_datetime(""))

    def test_garbage_returns_none(self):
        self.assertIsNone(media_automation._parse_jf_datetime("not-a-date"))


if __name__ == "__main__":
    unittest.main()
