"""Tests for the AI discovery source (v1.3.0) — Gemini-powered "ai" pseudo-list.

Covers the three failure-prone seams: lenient JSON parsing of grounded model
output (Gemini's JSON mode is incompatible with Google Search grounding, so the
contract is prompt-enforced only), suggestion→Trakt-ID resolution (hallucinated
or near-miss titles must not resolve to the wrong item), and the fetch_ai_list
orchestrator's caching + fail-loud-fall-through behavior. No real network.
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trakt_discovery


def _search_result(media_type, title, year, trakt_id=1, tmdb_id=100):
    """A Trakt /search result wrapper in the real API shape."""
    return {
        "type": media_type,
        "score": 1000,
        media_type: {
            "title": title,
            "year": year,
            "rating": 8.0,
            "votes": 5000,
            "genres": ["drama"],
            "ids": {"trakt": trakt_id, "tmdb": tmdb_id},
        },
    }


def _gemini_response(payload_text):
    """A mocked successful requests.Response for the Gemini endpoint."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": payload_text}]}}]
    }
    return resp


class AiJsonParsingTests(unittest.TestCase):
    """parse_ai_suggestions — must never raise, must normalize aliases."""

    def test_plain_json_array(self):
        text = '[{"title": "Severance", "year": 2022, "media_type": "show", "reason": "x"}]'
        result = trakt_discovery.parse_ai_suggestions(text)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "Severance")
        self.assertEqual(result[0]["year"], 2022)

    def test_markdown_fenced_array(self):
        text = '```json\n[{"title": "Dune", "year": 2021, "media_type": "movie"}]\n```'
        result = trakt_discovery.parse_ai_suggestions(text)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["media_type"], "movie")

    def test_prose_wrapped_array(self):
        text = ('Here are my picks based on current trends:\n'
                '[{"title": "The Bear", "year": 2022, "media_type": "show"}]\n'
                'Enjoy your watching!')
        result = trakt_discovery.parse_ai_suggestions(text)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "The Bear")

    def test_malformed_json_returns_empty(self):
        self.assertEqual(trakt_discovery.parse_ai_suggestions('[{"title": "Broken",]'), [])

    def test_no_array_returns_empty(self):
        self.assertEqual(trakt_discovery.parse_ai_suggestions("I cannot help with that."), [])

    def test_non_list_json_returns_empty(self):
        # A top-level object has no [ ... ] slice → no array found
        self.assertEqual(trakt_discovery.parse_ai_suggestions('{"recommendations": 1}'), [])

    def test_media_type_aliases_normalized(self):
        text = json.dumps([
            {"title": "A", "year": 2024, "media_type": "tv"},
            {"title": "B", "year": 2024, "media_type": "series"},
            {"title": "C", "year": 2024, "media_type": "film"},
        ])
        result = trakt_discovery.parse_ai_suggestions(text)
        self.assertEqual([s["media_type"] for s in result], ["show", "show", "movie"])

    def test_invalid_entries_dropped(self):
        text = json.dumps([
            {"title": "", "year": 2024, "media_type": "show"},        # empty title
            {"year": 2024, "media_type": "show"},                      # missing title
            {"title": "X", "year": 2024, "media_type": "podcast"},     # unknown type
            {"title": "Keep", "year": "2024", "media_type": "show"},   # string year → int
            {"title": "NoYear", "year": "soon", "media_type": "movie"},  # bogus year → None
        ])
        result = trakt_discovery.parse_ai_suggestions(text)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["title"], "Keep")
        self.assertEqual(result[0]["year"], 2024)
        self.assertIsNone(result[1]["year"])


class BuildPromptTests(unittest.TestCase):
    """build_ai_prompt — blocklist awareness so the model avoids filtered platforms."""

    def test_blocked_platforms_become_hard_exclusion(self):
        prompt = trakt_discovery.build_ai_prompt(
            history=[], trakt_trending=[], tmdb_trending=[], exclusions=[],
            n_shows=2, n_movies=2, blocked_platforms=["Netflix", "Apple TV"])
        self.assertIn("CANNOT use these platforms: Netflix, Apple TV", prompt)
        self.assertIn("do NOT".lower(), prompt.lower())

    def test_no_blocklist_omits_exclusion(self):
        prompt = trakt_discovery.build_ai_prompt(
            history=[], trakt_trending=[], tmdb_trending=[], exclusions=[],
            n_shows=2, n_movies=2, blocked_platforms=[])
        self.assertNotIn("CANNOT use these platforms", prompt)


class AiResolutionTests(unittest.TestCase):
    """resolve_ai_suggestion — two-pass Trakt search with strict-ish matching."""

    def test_exact_title_and_year(self):
        results = [_search_result("show", "Severance", 2022)]
        with patch.object(trakt_discovery, "trakt_get", return_value=results) as m:
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "Severance", "year": 2022, "media_type": "show"})
        self.assertIsNotNone(match)
        self.assertEqual(match["show"]["ids"]["trakt"], 1)
        m.assert_called_once()
        self.assertEqual(m.call_args.kwargs["params"]["years"], "2021-2023")

    def test_year_off_by_one_matches(self):
        results = [_search_result("movie", "Dune", 2021)]
        with patch.object(trakt_discovery, "trakt_get", return_value=results):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "Dune", "year": 2022, "media_type": "movie"})
        self.assertIsNotNone(match)

    def test_year_drift_falls_back_to_unconstrained_search(self):
        """Model gave current-season year; Trakt year is first-air. Pass 1 (year-
        constrained) finds nothing; pass 2 drops the year filter and matches."""
        good = [_search_result("show", "Taskmaster", 2015)]
        with patch.object(trakt_discovery, "trakt_get", side_effect=[[], good]) as m:
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "Taskmaster", "year": 2025, "media_type": "show"})
        self.assertIsNotNone(match)
        self.assertEqual(m.call_count, 2)
        self.assertIn("years", m.call_args_list[0].kwargs["params"])
        self.assertNotIn("years", m.call_args_list[1].kwargs["params"])

    def test_no_match_returns_none(self):
        with patch.object(trakt_discovery, "trakt_get", return_value=[]):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "Completely Hallucinated Show", "year": 2026, "media_type": "show"})
        self.assertIsNone(match)

    def test_regional_suffix_startswith_matching(self):
        results = [_search_result("show", "The Office (US)", 2005)]
        with patch.object(trakt_discovery, "trakt_get", return_value=results):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "The Office", "year": 2005, "media_type": "show"})
        self.assertIsNotNone(match)

    def test_unrelated_title_rejected(self):
        """Substring-anywhere must NOT match — only prefix overlap counts."""
        results = [_search_result("show", "Not Quite The Bear At All Show", 2022)]
        with patch.object(trakt_discovery, "trakt_get", return_value=results):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "The Bear", "year": 2022, "media_type": "show"})
        self.assertIsNone(match)

    def test_exact_match_preferred_over_prefix(self):
        """The 'Sugar' → 'Sugar Apple Fairy Tale' bug: an exact normalized-title
        match must win over a higher-ranked prefix near-match, even when the
        prefix result appears first in Trakt's relevance order."""
        results = [
            _search_result("show", "Sugar Apple Fairy Tale", 2023, trakt_id=99),
            _search_result("show", "Sugar", 2024, trakt_id=7),
        ]
        with patch.object(trakt_discovery, "trakt_get", return_value=results):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "Sugar", "year": 2024, "media_type": "show"})
        self.assertIsNotNone(match)
        self.assertEqual(match["show"]["ids"]["trakt"], 7)

    def test_prefix_fallback_when_no_exact(self):
        """With no exact match, the first acceptable prefix near-match is used."""
        results = [_search_result("show", "The Office (US)", 2005, trakt_id=5)]
        with patch.object(trakt_discovery, "trakt_get", return_value=results):
            match = trakt_discovery.resolve_ai_suggestion(
                {"title": "The Office", "year": 2005, "media_type": "show"})
        self.assertIsNotNone(match)
        self.assertEqual(match["show"]["ids"]["trakt"], 5)


class FetchAiListTests(unittest.TestCase):
    """fetch_ai_list — caching, unconfigured skip, fail-loud fall-through."""

    def setUp(self):
        trakt_discovery._ai_cache.clear()
        trakt_discovery._token_cache.clear()
        self._patches = [
            patch.object(trakt_discovery, "GEMINI_API_KEY", "test-key"),
            patch.object(trakt_discovery, "TRAKT_DISCOVER_SHOWS", True),
            patch.object(trakt_discovery, "TRAKT_DISCOVER_MOVIES", True),
            patch.object(trakt_discovery, "TRAKT_MAX_SHOW_REQUESTS", 5),
            patch.object(trakt_discovery, "TRAKT_MAX_MOVIE_REQUESTS", 5),
            patch.object(trakt_discovery, "fetch_watch_history_summary", return_value=[]),
            patch.object(trakt_discovery, "_trending_titles_for_prompt", return_value=[]),
            patch.object(trakt_discovery, "_tmdb_trending_titles", return_value=[]),
            patch.object(trakt_discovery, "fetch_ai_exclusions", return_value=[]),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        trakt_discovery._ai_cache.clear()

    def test_no_api_key_skips_without_calling_gemini(self):
        with patch.object(trakt_discovery, "GEMINI_API_KEY", ""), \
             patch.object(trakt_discovery.requests, "post") as post:
            self.assertEqual(trakt_discovery.fetch_ai_list(None, "show"), [])
            self.assertEqual(trakt_discovery.fetch_ai_list(None, "movie"), [])
        post.assert_not_called()

    def test_gemini_failure_alerts_once_and_falls_through(self):
        with patch.object(trakt_discovery.requests, "post",
                          side_effect=requests.exceptions.ConnectionError("boom")) as post, \
             patch.object(trakt_discovery, "_send_alert_once") as alert:
            self.assertEqual(trakt_discovery.fetch_ai_list(None, "show"), [])
            # Second media type must reuse the cached failure — no retry, no second alert
            self.assertEqual(trakt_discovery.fetch_ai_list(None, "movie"), [])
        self.assertEqual(post.call_count, 1)
        alert.assert_called_once()
        self.assertEqual(alert.call_args.kwargs.get("subject"), "AI discovery failure")

    def test_one_gemini_call_serves_both_types(self):
        payload = json.dumps([
            {"title": "Severance", "year": 2022, "media_type": "show"},
            {"title": "Dune", "year": 2021, "media_type": "movie"},
        ])
        side_effects = {
            "show": [_search_result("show", "Severance", 2022, trakt_id=1)],
            "movie": [_search_result("movie", "Dune", 2021, trakt_id=2)],
        }
        with patch.object(trakt_discovery.requests, "post",
                          return_value=_gemini_response(payload)) as post, \
             patch.object(trakt_discovery, "trakt_search",
                          side_effect=lambda mt, *a, **kw: side_effects[mt]):
            shows = trakt_discovery.fetch_ai_list(None, "show")
            movies = trakt_discovery.fetch_ai_list(None, "movie")
        self.assertEqual(post.call_count, 1)
        self.assertEqual(len(shows), 1)
        self.assertEqual(len(movies), 1)

    def test_resolved_item_is_pipeline_compatible(self):
        """The resolved wrapper must satisfy extract_item_info unchanged —
        that's the guarantee that every existing filter applies to AI picks."""
        payload = json.dumps([{"title": "Severance", "year": 2022, "media_type": "show"}])
        with patch.object(trakt_discovery.requests, "post",
                          return_value=_gemini_response(payload)), \
             patch.object(trakt_discovery, "trakt_search",
                          return_value=[_search_result("show", "Severance", 2022,
                                                       trakt_id=42, tmdb_id=4242)]):
            items = trakt_discovery.fetch_ai_list(None, "show")
        info = trakt_discovery.extract_item_info(items[0], "show", "ai")
        self.assertEqual(info["trakt_id"], 42)
        self.assertEqual(info["tmdb_id"], 4242)
        self.assertEqual(info["title"], "Severance")

    def test_fetch_list_delegates_ai_source(self):
        with patch.object(trakt_discovery, "fetch_ai_list", return_value=[]) as m:
            trakt_discovery.fetch_list(None, "ai", "show")
        m.assert_called_once_with(None, "show")


class GeminiRequestShapeTests(unittest.TestCase):
    """gemini_generate — request shape and response part handling."""

    def _call(self, web_search):
        with patch.object(trakt_discovery, "GEMINI_API_KEY", "test-key"), \
             patch.object(trakt_discovery, "AI_WEB_SEARCH", web_search), \
             patch.object(trakt_discovery.requests, "post",
                          return_value=_gemini_response("[]")) as post:
            trakt_discovery.gemini_generate("prompt")
        return post.call_args

    def test_web_search_enables_grounding_tool(self):
        call = self._call(web_search=True)
        self.assertEqual(call.kwargs["json"]["tools"], [{"google_search": {}}])

    def test_web_search_off_omits_tools(self):
        call = self._call(web_search=False)
        self.assertNotIn("tools", call.kwargs["json"])

    def test_api_key_in_header_not_url(self):
        call = self._call(web_search=False)
        self.assertEqual(call.kwargs["headers"]["x-goog-api-key"], "test-key")
        self.assertNotIn("test-key", call.args[0])

    def test_multipart_response_text_concatenated(self):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "[{\"a\""}, {"text": ": 1}]"}]}}]
        }
        with patch.object(trakt_discovery, "GEMINI_API_KEY", "test-key"), \
             patch.object(trakt_discovery.requests, "post", return_value=resp):
            text = trakt_discovery.gemini_generate("prompt")
        self.assertEqual(text, '[{"a": 1}]')

    def test_empty_candidates_raises(self):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"candidates": [], "promptFeedback": {"blockReason": "SAFETY"}}
        with patch.object(trakt_discovery, "GEMINI_API_KEY", "test-key"), \
             patch.object(trakt_discovery.requests, "post", return_value=resp):
            with self.assertRaises(ValueError):
                trakt_discovery.gemini_generate("prompt")


if __name__ == "__main__":
    unittest.main()
