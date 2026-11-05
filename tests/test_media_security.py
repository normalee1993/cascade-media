"""v1.11.0 media_automation.py security / integrity fixes.

S7 urllib3 debug logging, S8 redirects, S9 Retry-After clamp, S10 Seerr status,
S11 credential redaction, S13-S17/S20 auto-import safeguards, S18 unnumbered
playback item, S19 ambiguous series titles, S38 DRY_RUN writes nothing.
"""

import logging
import os
import sqlite3
import sys
import unittest
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import media_automation  # noqa: E402

BY_ID_MSG = "Found matching series via grab history, but release was matched to series by ID. Automatic import is not possible."


def _record(download_id="dl-1", messages=(BY_ID_MSG,), title="Some.Release", state="importBlocked"):
    return {"trackedDownloadState": state, "downloadId": download_id, "title": title,
            "statusMessages": [{"title": title, "messages": list(messages)}]}


def _cand(path="/dl/Show.S01E01.mkv", episode_ids=(101,), rejections=None, series_id=7):
    return {"path": path, "series": {"id": series_id}, "episodes": [{"id": e} for e in episode_ids],
            "quality": {}, "languages": [], "releaseGroup": "G", "rejections": rejections or []}


def _movie_cand(path="/dl/Movie.mkv", movie_id=55, rejections=None):
    return {"path": path, "movie": {"id": movie_id}, "quality": {}, "languages": [],
            "releaseGroup": "G", "rejections": rejections or []}


class _Resolver(unittest.TestCase):
    def setUp(self):
        for p in (patch.object(media_automation, "DRY_RUN", False),):
            p.start()
            self.addCleanup(p.stop)

    def run_sonarr(self, records, candidates):
        lookups = []

        def fake_get(path, *a, **k):
            if path.startswith("/queue"):
                return {"records": records, "totalRecords": len(records)}
            if path.startswith("/manualimport?downloadId="):
                lookups.append(path)
                return candidates
            raise AssertionError(path)

        with patch.object(media_automation, "sonarr_get", side_effect=fake_get), \
             patch.object(media_automation, "sonarr_post") as post:
            count = media_automation.resolve_blocked_imports()
        return count, post, lookups


class AutoImportSafeguardTests(_Resolver):
    def test_rejected_candidates_are_not_forced(self):
        count, post, _ = self.run_sonarr(
            [_record()], [_cand(rejections=[{"reason": "Not an upgrade for existing episode file(s)", "type": "permanent"}])])
        self.assertEqual(count, 0)
        post.assert_not_called()

    def test_sample_rejection_and_name_skipped_main_file_kept(self):
        count, post, _ = self.run_sonarr([_record()], [
            _cand(path="/dl/Show.S01E01.mkv"),
            _cand(path="/dl/Sample/show-sample.mkv"),
            _cand(path="/dl/x.mkv", rejections=[{"reason": "Sample", "type": "permanent"}]),
        ])
        self.assertEqual(count, 1)
        files = post.call_args[0][1]["files"]
        self.assertEqual([f["path"] for f in files], ["/dl/Show.S01E01.mkv"])

    def test_two_files_for_same_episode_skip_whole_download(self):
        count, post, _ = self.run_sonarr([_record()], [
            _cand(path="/dl/a.mkv", episode_ids=(101, 102)),
            _cand(path="/dl/b.mkv", episode_ids=(102,)),
        ])
        self.assertEqual(count, 0)
        post.assert_not_called()

    def test_two_files_for_same_movie_skip(self):
        def fake_get(path, *a, **k):
            if path.startswith("/queue"):
                return {"records": [_record(messages=["release was matched to movie by ID."])], "totalRecords": 1}
            return [_movie_cand(path="/dl/Movie.mkv"), _movie_cand(path="/dl/Movie.Extra.mkv")]
        with patch.object(media_automation, "radarr_get", side_effect=fake_get), \
             patch.object(media_automation, "radarr_post") as post:
            self.assertEqual(media_automation.resolve_blocked_imports_radarr(), 0)
        post.assert_not_called()

    def test_season_pack_records_fire_one_import(self):
        records = [_record(download_id="pack-1") for _ in range(10)]  # one per episode
        count, post, lookups = self.run_sonarr(records, [_cand()])
        self.assertEqual(count, 1)
        self.assertEqual(len(lookups), 1)
        post.assert_called_once()

    def test_download_id_is_url_encoded(self):
        _, _, lookups = self.run_sonarr([_record(download_id="a&seriesId=9#x")], [_cand()])
        self.assertEqual(lookups, ["/manualimport?downloadId=a%26seriesId%3D9%23x"])

    def test_release_name_cannot_trip_the_gate(self):
        """statusMessages[].title is the uploader-controlled release/file name."""
        rec = _record(messages=["Not a Custom Format upgrade"], title="x matched to series by id x")
        rec["statusMessages"] = [{"title": "Fake matched to series by ID", "messages": ["Sample"]}]
        self.assertFalse(media_automation._is_matched_by_id_block(rec))


class SeerrStatusTests(unittest.TestCase):
    def _results(self, *reqs):
        return {"results": list(reqs)}

    @staticmethod
    def _req(status, seasons, tvdb=111):
        return {"type": "tv", "status": status, "media": {"tvdbId": tvdb},
                "seasons": [{"seasonNumber": n} for n in seasons]}

    def test_declined_newer_request_ignored_for_approved_older(self):
        data = self._results(self._req(3, [1, 2, 3, 4]), self._req(2, [1]))
        with patch.object(media_automation, "SEERR_API_KEY", "k"), \
             patch.object(media_automation, "seerr_get", return_value=data):
            self.assertEqual(media_automation.get_requested_seasons_from_seerr(111, "Show"), {1})

    def test_only_pending_request_counts_as_none(self):
        data = self._results(self._req(1, [1, 2, 3]))
        with patch.object(media_automation, "SEERR_API_KEY", "k"), \
             patch.object(media_automation, "seerr_get", return_value=data):
            self.assertIsNone(media_automation.get_requested_seasons_from_seerr(111, "Show"))


class DryRunTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE processed_series (sonarr_id INTEGER PRIMARY KEY, title TEXT, status TEXT, processed_at TEXT)")
        self.conn.execute("CREATE TABLE unlocked_seasons (sonarr_id INTEGER, season_number INTEGER, unlocked_by TEXT, unlocked_at TEXT, PRIMARY KEY (sonarr_id, season_number))")

    def test_dry_run_writes_no_state_and_deletes_nothing(self):
        with patch.object(media_automation, "DRY_RUN", True), \
             patch.object(media_automation.requests, "delete") as delete:
            self.assertTrue(media_automation.claim_series_for_processing(self.conn, 5, "S"))
            media_automation.mark_series_done(self.conn, 5, "S")
            media_automation.mark_season_unlocked(self.conn, 5, 1, "t")
            media_automation.sonarr_delete("/queue/9?removeFromClient=true")
        delete.assert_not_called()
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM processed_series").fetchone()[0], 0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM unlocked_seasons").fetchone()[0], 0)


class RequestHelperTests(unittest.TestCase):
    @staticmethod
    def _resp(status, headers=None, body=None):
        r = MagicMock()
        r.status_code = status
        r.headers = headers or {}
        r.json.return_value = body if body is not None else {"ok": True}
        r.raise_for_status.return_value = None
        return r

    def test_cross_host_redirect_refused(self):
        method = MagicMock(return_value=self._resp(302, {"Location": "https://evil.example/collect"}))
        with self.assertRaises(requests.exceptions.RequestException):
            media_automation._api_request_with_retry(method, "http://sonarr:8989/api/v3/series", {"X-Api-Key": "k"}, max_retries=1)
        self.assertEqual(method.call_count, 1)
        self.assertFalse(method.call_args.kwargs["allow_redirects"])

    def test_same_host_https_upgrade_followed(self):
        method = MagicMock(side_effect=[
            self._resp(301, {"Location": "https://sonarr:8989/api/v3/series"}),
            self._resp(200, body=[1]),
        ])
        out = media_automation._api_request_with_retry(method, "http://sonarr:8989/api/v3/series", {}, max_retries=1)
        self.assertEqual(out, [1])
        self.assertEqual(method.call_args_list[1].args[0], "https://sonarr:8989/api/v3/series")

    def test_retry_after_is_clamped(self):
        for raw, expected in (("99999999999", 120), ("-5", 0)):
            method = MagicMock(side_effect=[self._resp(429, {"Retry-After": raw}), self._resp(200)])
            with patch.object(media_automation.time, "sleep") as sleep:
                media_automation._api_request_with_retry(method, "http://x", {})
            sleep.assert_called_once_with(expected)

    def test_url_credentials_and_keys_never_logged_or_reraised(self):
        url = "http://admin:Hunter2Pass@sonarr:8989/api/v3/series?apikey=TOPSECRET"

        def boom(u, **kw):
            raise requests.exceptions.ConnectionError(f"Max retries exceeded with url: {u}")

        with self.assertLogs(media_automation.log, level="ERROR") as cm, \
             patch.object(media_automation.time, "sleep"):
            with self.assertRaises(requests.exceptions.ConnectionError) as ctx:
                media_automation._api_request_with_retry(boom, url, {}, max_retries=2)
        text = "\n".join(cm.output) + str(ctx.exception)
        self.assertNotIn("Hunter2Pass", text)
        self.assertNotIn("TOPSECRET", text)

    def test_urllib3_debug_logging_suppressed(self):
        self.assertGreaterEqual(logging.getLogger("urllib3").level, logging.WARNING)


class PlaybackRobustnessTests(unittest.TestCase):
    def test_unnumbered_item_does_not_abort_loop(self):
        sessions = [
            {"UserId": "u1", "NowPlayingItem": {"Type": "Episode", "IndexNumber": None,
                                                "ParentIndexNumber": None, "SeriesName": "Special"}},
            {"UserId": "u1", "NowPlayingItem": {"Type": "Episode", "IndexNumber": 1,
                                                "ParentIndexNumber": 2, "SeriesName": "Show", "SeriesId": "j1"}},
        ]
        conn = MagicMock()
        with patch.object(media_automation, "JELLYFIN_USER_IDS", ["u1"]), \
             patch.object(media_automation, "jellyfin_get", return_value=sessions), \
             patch.object(media_automation, "sonarr_get", side_effect=[
                 [{"id": 3, "title": "Show", "tvdbId": 9}], [{"seasonNumber": 2}]]), \
             patch.object(media_automation, "is_season_unlocked", return_value=False), \
             patch.object(media_automation, "unlock_and_download_season") as unlock:
            media_automation.check_active_playback(conn)
        unlock.assert_called_once()

    def test_ambiguous_title_prefers_tvdb(self):
        bare = {"id": 1, "title": "Battlestar Galactica", "tvdbId": 71173}
        remake = {"id": 2, "title": "Battlestar Galactica (2003)", "tvdbId": 73545}
        by_title = {"battlestar galactica": bare, "battlestar galactica (2003)": remake}
        by_tvdb = {71173: bare, 73545: remake}
        with patch.object(media_automation, "jellyfin_get", return_value={"ProviderIds": {"Tvdb": "73545"}}):
            got = media_automation._resolve_sonarr_series("Battlestar Galactica", "jf", "u", by_title, by_tvdb)
        self.assertIs(got, remake)

    def test_unambiguous_title_needs_no_lookup(self):
        show = {"id": 1, "title": "Severance", "tvdbId": 1}
        with patch.object(media_automation, "jellyfin_get") as jf:
            got = media_automation._resolve_sonarr_series("Severance", "jf", "u", {"severance": show}, {1: show})
        self.assertIs(got, show)
        jf.assert_not_called()


if __name__ == "__main__":
    unittest.main()
