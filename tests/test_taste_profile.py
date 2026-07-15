"""Tests for the statistical taste profile (v1.10.0) — taste_profile.py.

Covers the pure math (recency decay, weighted shares/medians), the
behavioral classifiers (binge/abandon boundaries), the incremental TMDB
metadata cache, Jellyfin user resolution, and the fail-soft orchestrator.
No real network; sqlite runs in-memory.
"""

import json
import os
import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import taste_profile

NOW = datetime(2026, 7, 14, tzinfo=timezone.utc)


def _watched(media_type, title, year=2024, genres=("drama",), plays=1,
             days_ago=0, tmdb_id=None):
    return {
        "plays": plays,
        "last_watched_at": (NOW - timedelta(days=days_ago)).isoformat(),
        media_type: {"title": title, "year": year, "genres": list(genres),
                     "ids": {"tmdb": tmdb_id}},
    }


class DecayWeightTests(unittest.TestCase):
    def test_today_is_full_weight(self):
        w = taste_profile._decay_weight(NOW.isoformat(), NOW, 90)
        self.assertAlmostEqual(w, 1.0, places=3)

    def test_one_half_life_is_half(self):
        when = (NOW - timedelta(days=90)).isoformat()
        self.assertAlmostEqual(taste_profile._decay_weight(when, NOW, 90), 0.5, places=3)

    def test_missing_or_garbage_date_gets_token_weight(self):
        self.assertEqual(taste_profile._decay_weight(None, NOW, 90), 0.1)
        self.assertEqual(taste_profile._decay_weight("not-a-date", NOW, 90), 0.1)


class ComputeProfileTests(unittest.TestCase):
    def test_recent_genre_outweighs_stale_genre(self):
        """One crime watch today must outrank many drama watches from years
        ago — the whole point of recency weighting."""
        watched = {"show": [_watched("show", f"Old Drama {i}", genres=("drama",),
                                     days_ago=1000) for i in range(5)]
                   + [_watched("show", "Fresh Crime", genres=("crime",), days_ago=1)],
                   "movie": []}
        profile = taste_profile.compute_profile(watched, {}, None, now=NOW)
        self.assertEqual(profile["genres"][0][0], "crime")

    def test_avoided_genres_lists_absent_majors_only(self):
        watched = {"movie": [_watched("movie", "M", genres=("horror",))], "show": []}
        profile = taste_profile.compute_profile(watched, {}, None, now=NOW)
        self.assertNotIn("horror", profile["avoided_genres"])
        self.assertIn("reality", profile["avoided_genres"])

    def test_rewatched_movies_and_people_from_metadata(self):
        watched = {"movie": [
            _watched("movie", "Heat", plays=3, days_ago=10, tmdb_id=949),
            _watched("movie", "Ronin", plays=1, days_ago=5, tmdb_id=8195),
        ], "show": []}
        metadata = {
            (949, "movie"): {"keywords": ["heist"], "cast": ["Robert De Niro"],
                             "directors": ["Michael Mann"], "runtime": 170},
            (8195, "movie"): {"keywords": ["heist"], "cast": ["Robert De Niro"],
                              "directors": ["John Frankenheimer"], "runtime": 122},
        }
        profile = taste_profile.compute_profile(watched, metadata, None, now=NOW)
        self.assertIn("Heat", profile["rewatched"])
        self.assertEqual(profile["keywords"][0][0], "heist")
        # De Niro appears in 2 titles (people need >=2); single-title directors don't
        people = [p for p, _ in profile["people"]]
        self.assertEqual(people, ["Robert De Niro"])

    def test_weighted_median_runtime(self):
        watched = {"movie": [
            _watched("movie", "A", days_ago=0, tmdb_id=1),
            _watched("movie", "B", days_ago=0, tmdb_id=2),
            _watched("movie", "C", days_ago=0, tmdb_id=3),
        ], "show": []}
        metadata = {(i, "movie"): {"keywords": [], "cast": [], "directors": [],
                                   "runtime": rt}
                    for i, rt in ((1, 90), (2, 110), (3, 150))}
        profile = taste_profile.compute_profile(watched, metadata, None, now=NOW)
        self.assertEqual(profile["runtime_movie"], 110)

    def test_hyphenated_trakt_slugs_normalize(self):
        """Regression (caught in DRY_RUN): Trakt sends 'science-fiction';
        without normalization it never matched the majors set, so sci-fi was
        declared 'never watched' for a household that binged Dark Matter."""
        watched = {"show": [_watched("show", "Dark Matter",
                                     genres=("science-fiction",))], "movie": []}
        profile = taste_profile.compute_profile(watched, {}, None, now=NOW)
        self.assertEqual(profile["genres"][0][0], "science fiction")
        self.assertNotIn("science fiction", profile["avoided_genres"])

    def test_rewatched_sort_survives_none_years(self):
        """Regression (caught in DRY_RUN): Jellyfin rewatch entries carry
        year=None; ties on (plays, title) must not compare int < None."""
        watched = {"movie": [
            _watched("movie", "Heat", plays=2, year=1995),
            _watched("movie", "Heat", plays=2, year=None),
        ], "show": []}
        jf = {"movies": {"Alien": {"plays": 3, "favorite": False}}, "series": {}}
        profile = taste_profile.compute_profile(watched, {}, jf, now=NOW)
        self.assertEqual(profile["rewatched"][0], "Alien")

    def test_ratings_gate(self):
        """Fewer than MIN_RATINGS_TO_USE ratings must be ignored entirely."""
        watched = {"movie": [_watched("movie", "M")], "show": []}
        few = [{"type": "movie", "rating": 10, "movie": {"title": "M"}}] * 5
        profile = taste_profile.compute_profile(watched, {}, None, ratings=few, now=NOW)
        self.assertEqual(profile["loved"], [])
        many = [{"type": "movie", "rating": 10, "movie": {"title": f"T{i}"}}
                for i in range(10)]
        profile = taste_profile.compute_profile(watched, {}, None, ratings=many, now=NOW)
        self.assertEqual(len(profile["loved"]), 10)


class PaceClassifierTests(unittest.TestCase):
    """Only the positive binge signal survives. Low completion is NOT a
    negative signal (this household's half-watched shows are backlog, not
    dislikes), and there is no 'abandoned' key any more."""

    def _signals(self, available, played, first_days_ago, last_days_ago):
        return {"movies": {}, "series": {"S": {
            "available": available, "played": played, "favorite": False,
            "first": (NOW - timedelta(days=first_days_ago)).isoformat(),
            "last": (NOW - timedelta(days=last_days_ago)).isoformat(),
        }}}

    def _binged(self, signals):
        profile = taste_profile.compute_profile({"show": [], "movie": []}, {},
                                                signals, now=NOW)
        self.assertNotIn("abandoned", profile)  # negative signal is gone
        return profile["binged"]

    def test_fast_complete_is_binged(self):
        self.assertEqual(self._binged(self._signals(8, 8, 10, 5)), ["S"])

    def test_low_completion_long_idle_is_not_flagged(self):
        """The old 'abandoned → avoid similar' case: must now produce nothing
        (it's backlog, and the profile carries no negative for it)."""
        self.assertEqual(self._binged(self._signals(10, 3, 200, 90)), [])

    def test_recent_low_completion_is_not_binged(self):
        self.assertEqual(self._binged(self._signals(10, 3, 20, 5)), [])

    def test_short_series_exempt(self):
        self.assertEqual(self._binged(self._signals(3, 3, 10, 9)), [])

    def test_slow_complete_is_not_binged(self):
        self.assertEqual(self._binged(self._signals(8, 8, 60, 5)), [])


class MetadataCacheTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

    def _tmdb(self, calls):
        def fake(endpoint, params=None):
            calls.append(endpoint)
            return {"keywords": {"keywords": [{"name": "Heist"}]},
                    "credits": {"cast": [{"name": "A"}],
                                "crew": [{"name": "D", "job": "Director"}]},
                    "runtime": 100}
        return fake

    def test_incremental_no_refetch(self):
        watched = {"movie": [_watched("movie", "Heat", tmdb_id=949)], "show": []}
        calls = []
        taste_profile.collect_title_metadata(self.conn, watched, self._tmdb(calls))
        taste_profile.collect_title_metadata(self.conn, watched, self._tmdb(calls))
        self.assertEqual(len(calls), 1)
        meta = taste_profile.load_metadata(self.conn)
        self.assertEqual(meta[(949, "movie")]["keywords"], ["heist"])
        self.assertEqual(meta[(949, "movie")]["directors"], ["D"])

    def test_fetch_cap_respected(self):
        watched = {"movie": [_watched("movie", f"M{i}", tmdb_id=i)
                             for i in range(1, 60)], "show": []}
        calls = []
        n = taste_profile.collect_title_metadata(self.conn, watched,
                                                 self._tmdb(calls), max_fetches=40)
        self.assertEqual(n, 40)
        self.assertEqual(len(calls), 40)

    def test_failed_fetch_skips_title(self):
        watched = {"movie": [_watched("movie", "Heat", tmdb_id=949)], "show": []}
        n = taste_profile.collect_title_metadata(self.conn, watched,
                                                 lambda e, params=None: None)
        self.assertEqual(n, 0)
        self.assertEqual(taste_profile.load_metadata(self.conn), {})


class JellyfinUserResolutionTests(unittest.TestCase):
    def test_guid_passes_through_without_api_call(self):
        guid = "a" * 32
        with patch.object(taste_profile, "_jellyfin_get") as m:
            self.assertEqual(
                taste_profile.resolve_jellyfin_user("http://jf", "k", guid), guid)
        m.assert_not_called()

    def test_name_resolves_case_insensitively(self):
        users = [{"Name": "AzureAperture", "Id": "abc123"}]
        with patch.object(taste_profile, "_jellyfin_get", return_value=users):
            self.assertEqual(
                taste_profile.resolve_jellyfin_user("http://jf", "k", "azureaperture"),
                "abc123")

    def test_unknown_name_returns_none(self):
        with patch.object(taste_profile, "_jellyfin_get", return_value=[]):
            self.assertIsNone(
                taste_profile.resolve_jellyfin_user("http://jf", "k", "nobody"))


class GenreLiftTests(unittest.TestCase):
    """Lift vs. a baseline population — the 'what's distinctive' signal."""

    def test_lift_computed_against_baseline(self):
        # 3 crime + 1 drama watched; baseline is drama-heavy, crime-light.
        watched = {"movie": [_watched("movie", f"C{i}", genres=("crime",))
                             for i in range(3)]
                   + [_watched("movie", "D", genres=("drama",))], "show": []}
        baseline = {"crime": 0.1, "drama": 0.6}
        profile = taste_profile.compute_profile(watched, {}, None, now=NOW,
                                                baseline_genres=baseline)
        genres = {g: lift for g, _, lift in profile["genres"]}
        # crime: user share .75 vs baseline .10 -> big lift; drama below baseline
        self.assertGreater(genres["crime"], 2.0)
        self.assertLess(genres["drama"], 1.0)

    def test_lift_none_without_baseline(self):
        watched = {"movie": [_watched("movie", "C", genres=("crime",))], "show": []}
        profile = taste_profile.compute_profile(watched, {}, None, now=NOW)
        self.assertTrue(all(lift is None for _, _, lift in profile["genres"]))

    def test_render_shows_lift_only_when_distinctive(self):
        watched = {"movie": [_watched("movie", f"C{i}", genres=("crime",))
                             for i in range(3)], "show": []}
        text = taste_profile.render_profile(
            taste_profile.compute_profile(watched, {}, None, now=NOW,
                                          baseline_genres={"crime": 0.05}))
        self.assertIn("× vs trending", text)

    def test_trending_baseline_maps_tmdb_ids(self):
        def fake_tmdb(endpoint, params=None):
            if endpoint.endswith("movie/week"):
                return {"results": [{"genre_ids": [80, 18]}]}  # crime, drama
            return {"results": [{"genre_ids": [10765]}]}       # tv sci-fi&fantasy
        base = taste_profile.trending_genre_baseline(fake_tmdb)
        self.assertAlmostEqual(base["crime"], 1/3)
        self.assertAlmostEqual(base["science fiction"], 1/3)


class JellyfinSignalsTests(unittest.TestCase):
    """fetch_jellyfin_signals — null-Name guard + show-level favorite seeding."""

    def _responses(self, movies, episodes, series):
        payloads = {"Movie": movies, "Episode": episodes, "Series": series}
        def fake(url, key, endpoint, params=None):
            return {"Items": payloads[params["IncludeItemTypes"]]}
        return fake

    def test_nameless_movie_skipped(self):
        movies = [{"UserData": {"PlayCount": 2, "IsFavorite": True}},   # no Name
                  {"Name": "Heat", "UserData": {"PlayCount": 3}}]
        with patch.object(taste_profile, "_jellyfin_get",
                          side_effect=self._responses(movies, [], [])):
            sig = taste_profile.fetch_jellyfin_signals("http://jf", "k", "uid")
        self.assertEqual(list(sig["movies"]), ["Heat"])   # None key never stored

    def test_show_level_favorite_seeded(self):
        episodes = [{"SeriesName": "Dexter", "UserData": {"Played": True}}]
        series = [{"Name": "Dexter", "UserData": {"IsFavorite": True}}]
        with patch.object(taste_profile, "_jellyfin_get",
                          side_effect=self._responses([], episodes, series)):
            sig = taste_profile.fetch_jellyfin_signals("http://jf", "k", "uid")
        self.assertTrue(sig["series"]["Dexter"]["favorite"])


class RenderProfileTests(unittest.TestCase):
    def test_renders_only_populated_sections(self):
        profile = taste_profile.compute_profile(
            {"movie": [_watched("movie", "Heat", genres=("crime",))], "show": []},
            {}, None, now=NOW)
        text = taste_profile.render_profile(profile)
        self.assertIn("TASTE PROFILE", text)
        self.assertIn("crime", text)
        self.assertNotIn("Binged fast", text)
        self.assertNotIn("Rewatched", text)
        self.assertNotIn("abandoned", text.lower())


class OrchestratorTests(unittest.TestCase):
    def test_any_failure_returns_none(self):
        """A crash anywhere inside must degrade to a profile-less prompt."""
        result = taste_profile.build_and_render(
            conn=None, watched={"show": [], "movie": []},
            tmdb_get=None, trakt_get=None, jellyfin_url="", jellyfin_api_key="",
            jellyfin_user="")
        self.assertIsNone(result)

    def test_empty_history_returns_none(self):
        conn = sqlite3.connect(":memory:")
        result = taste_profile.build_and_render(
            conn=conn, watched={"show": [], "movie": []},
            tmdb_get=lambda e, params=None: None,
            trakt_get=lambda e, **kw: [],
            jellyfin_url="", jellyfin_api_key="", jellyfin_user="")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
