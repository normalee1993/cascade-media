"""Tests for Phase 3 — "matched by ID" auto-import (Sonarr + Radarr).

Covers the failure-prone seams:
  * The by-ID signature matcher: acts on the "matched ... by ID" import block
    and IGNORES the two benign cases (season-pack "No files are eligible for
    import" and quality "Not a Custom Format upgrade").
  * The /manualimport -> ManualImport command flow: candidate rows are carried
    through into the `files` payload with importMode "auto", Sonarr using
    seriesId/episodeIds and Radarr using movieId.
  * DRY_RUN identifies items but fires no command.
  * The new Radarr client request shape (URL, X-Api-Key header).
  * No-op when the relevant *_API_KEY is unset, or AUTO_IMPORT_BLOCKED is off.

All external seams are mocked; no real network is touched.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import media_automation


# ---- queue-record / candidate factories ----------------------------------

def _record(state="importBlocked", messages=None, download_id="dl-1", title="Some.Release"):
    """A Sonarr/Radarr /queue record in the shape the resolver consumes."""
    status_messages = []
    if messages:
        status_messages = [{"title": "Import", "messages": list(messages)}]
    return {
        "trackedDownloadState": state,
        "statusMessages": status_messages,
        "downloadId": download_id,
        "title": title,
    }


BY_ID_MSG = "Episode title matched to series by ID. Automatic import is not possible. Manual Import required."
NO_FILES_MSG = "No files are eligible for import in ."
NOT_CF_MSG = "Not a Custom Format upgrade for existing episode file(s)."


def _sonarr_candidate(path="/dl/ep.mkv", series_id=7, episode_ids=(101,)):
    return {
        "path": path,
        "series": {"id": series_id, "title": "Battlestar Galactica"},
        "episodes": [{"id": eid} for eid in episode_ids],
        "quality": {"quality": {"id": 4, "name": "HDTV-720p"}},
        "languages": [{"id": 1, "name": "English"}],
        "releaseGroup": "GRP",
        "customFormats": [],
        "indexerFlags": 0,
    }


def _radarr_candidate(path="/dl/movie.mkv", movie_id=55):
    return {
        "path": path,
        "movie": {"id": movie_id, "title": "Batman: The Long Halloween"},
        "quality": {"quality": {"id": 7, "name": "Bluray-1080p"}},
        "languages": [{"id": 1, "name": "English"}],
        "releaseGroup": "GRP",
        "customFormats": [],
        "indexerFlags": 0,
    }


class SignatureMatcherTests(unittest.TestCase):
    """_is_matched_by_id_block — conservative by-ID detection."""

    def test_matches_by_id_block(self):
        self.assertTrue(media_automation._is_matched_by_id_block(
            _record(messages=[BY_ID_MSG])))

    def test_matches_import_pending_state(self):
        self.assertTrue(media_automation._is_matched_by_id_block(
            _record(state="importPending", messages=[BY_ID_MSG])))

    def test_ignores_no_files_eligible(self):
        self.assertFalse(media_automation._is_matched_by_id_block(
            _record(messages=[NO_FILES_MSG])))

    def test_ignores_not_custom_format_upgrade(self):
        self.assertFalse(media_automation._is_matched_by_id_block(
            _record(messages=[NOT_CF_MSG])))

    def test_ignores_when_state_not_blocked(self):
        # Right message but the item is still downloading -> not a block yet.
        self.assertFalse(media_automation._is_matched_by_id_block(
            _record(state="downloading", messages=[BY_ID_MSG])))

    def test_ignores_empty_messages(self):
        self.assertFalse(media_automation._is_matched_by_id_block(_record(messages=[])))

    def test_by_id_wins_even_if_benign_text_also_present(self):
        # If a record somehow carried both, the by-ID signal should still match
        # but the explicit benign guards must veto it.
        self.assertFalse(media_automation._is_matched_by_id_block(
            _record(messages=[BY_ID_MSG, NO_FILES_MSG])))


class SonarrResolveTests(unittest.TestCase):
    """resolve_blocked_imports — Sonarr manualimport -> ManualImport flow."""

    def setUp(self):
        self._cfg = [
            patch.object(media_automation, "DRY_RUN", False),
            patch.object(media_automation, "SONARR_API_KEY", "son-key"),
            patch.object(media_automation, "AUTO_IMPORT_BLOCKED", True),
        ]
        for p in self._cfg:
            p.start()

    def tearDown(self):
        for p in self._cfg:
            p.stop()

    def _get(self, candidates):
        def fake(path, *a, **k):
            if path.startswith("/queue"):
                return {"records": [_record(messages=[BY_ID_MSG], download_id="dl-1")],
                        "totalRecords": 1}
            if path.startswith("/manualimport?downloadId="):
                return candidates
            raise AssertionError(f"unexpected sonarr_get path: {path}")
        return fake

    def test_payload_shape_carries_files_and_auto_mode(self):
        with patch.object(media_automation, "sonarr_get",
                          side_effect=self._get([_sonarr_candidate()])), \
             patch.object(media_automation, "sonarr_post") as post:
            count = media_automation.resolve_blocked_imports()

        self.assertEqual(count, 1)
        post.assert_called_once()
        endpoint, payload = post.call_args[0][0], post.call_args[0][1]
        self.assertEqual(endpoint, "/command")
        self.assertEqual(payload["name"], "ManualImport")
        self.assertEqual(payload["importMode"], "auto")
        self.assertEqual(len(payload["files"]), 1)
        f = payload["files"][0]
        self.assertEqual(f["seriesId"], 7)
        self.assertEqual(f["episodeIds"], [101])
        self.assertEqual(f["downloadId"], "dl-1")
        self.assertIn("quality", f)

    def test_skips_candidate_without_episodes(self):
        bad = _sonarr_candidate(episode_ids=())
        with patch.object(media_automation, "sonarr_get",
                          side_effect=self._get([bad])), \
             patch.object(media_automation, "sonarr_post") as post:
            count = media_automation.resolve_blocked_imports()
        self.assertEqual(count, 0)
        post.assert_not_called()

    def test_dry_run_fires_nothing(self):
        with patch.object(media_automation, "DRY_RUN", True), \
             patch.object(media_automation, "sonarr_post") as post:
            # In DRY_RUN the resolver should never reach the manualimport lookup
            # or the command; only the queue is paged.
            def fake_get(path, *a, **k):
                if path.startswith("/queue"):
                    return {"records": [_record(messages=[BY_ID_MSG])], "totalRecords": 1}
                raise AssertionError(f"DRY_RUN should not call: {path}")
            with patch.object(media_automation, "sonarr_get", side_effect=fake_get):
                count = media_automation.resolve_blocked_imports()
        self.assertEqual(count, 0)
        post.assert_not_called()

    def test_benign_records_skipped(self):
        def fake_get(path, *a, **k):
            if path.startswith("/queue"):
                return {"records": [
                    _record(messages=[NO_FILES_MSG], download_id="dl-a"),
                    _record(messages=[NOT_CF_MSG], download_id="dl-b"),
                ], "totalRecords": 2}
            raise AssertionError(f"should not look up manualimport: {path}")
        with patch.object(media_automation, "sonarr_get", side_effect=fake_get), \
             patch.object(media_automation, "sonarr_post") as post:
            count = media_automation.resolve_blocked_imports()
        self.assertEqual(count, 0)
        post.assert_not_called()


class RadarrResolveTests(unittest.TestCase):
    """resolve_blocked_imports_radarr — movieId flow + client request shape."""

    def setUp(self):
        self._cfg = [
            patch.object(media_automation, "DRY_RUN", False),
            patch.object(media_automation, "RADARR_API_KEY", "rad-key"),
            patch.object(media_automation, "AUTO_IMPORT_BLOCKED", True),
        ]
        for p in self._cfg:
            p.start()

    def tearDown(self):
        for p in self._cfg:
            p.stop()

    def test_payload_uses_movie_id(self):
        def fake_get(path, *a, **k):
            if path.startswith("/queue"):
                return {"records": [_record(messages=[BY_ID_MSG], download_id="rdl-1")],
                        "totalRecords": 1}
            if path.startswith("/manualimport?downloadId="):
                return [_radarr_candidate()]
            raise AssertionError(f"unexpected radarr_get path: {path}")

        with patch.object(media_automation, "radarr_get", side_effect=fake_get), \
             patch.object(media_automation, "radarr_post") as post:
            count = media_automation.resolve_blocked_imports_radarr()

        self.assertEqual(count, 1)
        payload = post.call_args[0][1]
        self.assertEqual(payload["name"], "ManualImport")
        self.assertEqual(payload["importMode"], "auto")
        f = payload["files"][0]
        self.assertEqual(f["movieId"], 55)
        self.assertNotIn("seriesId", f)
        self.assertNotIn("episodeIds", f)
        self.assertEqual(f["downloadId"], "rdl-1")

    def test_radarr_client_request_shape(self):
        """radarr_get hits {RADARR_URL}/api/v3<endpoint> with the X-Api-Key header."""
        captured = {}

        def fake_request(method, url, headers, **kwargs):
            captured["url"] = url
            captured["headers"] = headers
            return {"records": []}

        with patch.object(media_automation, "RADARR_URL", "http://radarr:7878"), \
             patch.object(media_automation, "RADARR_HEADERS",
                          {"X-Api-Key": "rad-key", "Content-Type": "application/json"}), \
             patch.object(media_automation, "_api_request_with_retry",
                          side_effect=fake_request):
            media_automation.radarr_get("/queue?page=1")

        self.assertEqual(captured["url"], "http://radarr:7878/api/v3/queue?page=1")
        self.assertEqual(captured["headers"]["X-Api-Key"], "rad-key")


class GatingTests(unittest.TestCase):
    """resolve_all_blocked_imports — key + master-toggle gating."""

    def test_noop_when_toggle_off(self):
        with patch.object(media_automation, "AUTO_IMPORT_BLOCKED", False), \
             patch.object(media_automation, "SONARR_API_KEY", "son-key"), \
             patch.object(media_automation, "RADARR_API_KEY", "rad-key"), \
             patch.object(media_automation, "resolve_blocked_imports") as son, \
             patch.object(media_automation, "resolve_blocked_imports_radarr") as rad:
            media_automation.resolve_all_blocked_imports()
        son.assert_not_called()
        rad.assert_not_called()

    def test_noop_when_keys_unset(self):
        with patch.object(media_automation, "AUTO_IMPORT_BLOCKED", True), \
             patch.object(media_automation, "SONARR_API_KEY", ""), \
             patch.object(media_automation, "RADARR_API_KEY", ""), \
             patch.object(media_automation, "resolve_blocked_imports") as son, \
             patch.object(media_automation, "resolve_blocked_imports_radarr") as rad:
            media_automation.resolve_all_blocked_imports()
        son.assert_not_called()
        rad.assert_not_called()

    def test_sonarr_only_when_only_sonarr_key(self):
        with patch.object(media_automation, "AUTO_IMPORT_BLOCKED", True), \
             patch.object(media_automation, "SONARR_API_KEY", "son-key"), \
             patch.object(media_automation, "RADARR_API_KEY", ""), \
             patch.object(media_automation, "resolve_blocked_imports") as son, \
             patch.object(media_automation, "resolve_blocked_imports_radarr") as rad:
            media_automation.resolve_all_blocked_imports()
        son.assert_called_once()
        rad.assert_not_called()

    def test_failure_in_one_does_not_abort_other(self):
        with patch.object(media_automation, "AUTO_IMPORT_BLOCKED", True), \
             patch.object(media_automation, "SONARR_API_KEY", "son-key"), \
             patch.object(media_automation, "RADARR_API_KEY", "rad-key"), \
             patch.object(media_automation, "resolve_blocked_imports",
                          side_effect=RuntimeError("boom")) as son, \
             patch.object(media_automation, "resolve_blocked_imports_radarr") as rad:
            # Must not raise; Radarr still runs.
            media_automation.resolve_all_blocked_imports()
        son.assert_called_once()
        rad.assert_called_once()


if __name__ == "__main__":
    unittest.main()
