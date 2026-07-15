"""Statistical taste profile for AI discovery (v1.10.0).

Distills the household's watch behavior into a compact natural-language
block for the Gemini prompt. Research on LLM recommenders shows a distilled
profile ALONGSIDE raw recent history beats either alone, so this module
supplements — not replaces — the RECENT WATCH HISTORY section.

Everything here runs locally BEFORE anything is sent to Gemini/Google:
raw play events, per-user Jellyfin state, and TMDB metadata stay on the
box; only the rendered profile text (genres, themes, names, title lists)
leaves it — the same class of data the prompt already contains.

Sources:
- Trakt watched history (title/year/genres/plays/last_watched) — the base
  taste signal, passed in by trakt_discovery's per-cycle cached fetch.
- TMDB keywords + credits, cached incrementally in the ai_title_metadata
  table (max AI_PROFILE_TMDB_FETCHES_PER_CYCLE new lookups per cycle, so
  the profile matures over the first week without a call burst).
- Jellyfin play state for ONE selected user (AI_PROFILE_JELLYFIN_USER):
  binge/favorite/rewatch are per-person signals — blending five household
  accounts would smear them into noise.
- Trakt personal ratings, used only if the account actually rates
  (>= MIN_RATINGS_TO_USE entries).

All statistics are pure stdlib. Every count is weighted by recency decay
w = 0.5 ** (days_since_watch / half_life_days), so the profile tracks
current taste, not 2019's.
"""

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

log = logging.getLogger("taste_profile")

# Below this many personal ratings, the ratings section is skipped — a
# handful of ratings says more about forgetting to rate than about taste.
MIN_RATINGS_TO_USE = 10

# Series shorter than this many downloaded episodes can't meaningfully be
# "binged" — a 3-episode miniseries watched in one sitting says nothing that
# plain watch history doesn't.
MIN_EPISODES_FOR_PACE = 4

BINGE_COMPLETION = 0.9     # >=90% watched...
BINGE_WINDOW_DAYS = 7      # ...within a week of first play = strong like
# NB: we deliberately do NOT infer dislike from low completion + long idle.
# This household starts many shows and circles back later, so a half-watched
# idle series is backlog, not a thumbs-down. Genuine dislikes come only from
# explicit Trakt ratings (<=5).


# ============================================================
# METADATA CACHE (TMDB keywords/credits, incremental)
# ============================================================
def ensure_metadata_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_title_metadata (
            tmdb_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            title TEXT,
            keywords TEXT,      -- JSON array of lowercase keyword strings
            cast_names TEXT,    -- JSON array, top billed
            directors TEXT,     -- JSON array (movies: director; shows: creators)
            runtime INTEGER,    -- minutes (movies: feature; shows: episode)
            collected_at TEXT NOT NULL,
            PRIMARY KEY (tmdb_id, media_type)
        )
    """)
    conn.commit()


def collect_title_metadata(conn, watched, tmdb_get, max_fetches=40):
    """Top up the ai_title_metadata cache from TMDB for watched titles that
    don't have a row yet. Incremental by design: at most max_fetches lookups
    per cycle, newest watches first (they dominate the decayed stats anyway).
    Returns the number fetched. Failures skip the title — a partial cache
    only thins the profile, never breaks it."""
    ensure_metadata_table(conn)
    cached = {(r[0], r[1]) for r in
              conn.execute("SELECT tmdb_id, media_type FROM ai_title_metadata")}
    fetched = 0
    for media_type in ("show", "movie"):
        for item in (watched.get(media_type) or []):
            if fetched >= max_fetches:
                break
            media = item.get(media_type, {})
            tmdb_id = (media.get("ids") or {}).get("tmdb")
            if not tmdb_id or (tmdb_id, media_type) in cached:
                continue
            endpoint = ("/tv" if media_type == "show" else "/movie") + f"/{tmdb_id}"
            data = tmdb_get(endpoint, params={"append_to_response": "credits,keywords"})
            if not data:
                continue
            # Keywords nest differently per type: movies under "keywords",
            # shows under "results".
            kw_block = data.get("keywords", {})
            keywords = [k["name"].lower()
                        for k in (kw_block.get("keywords") or kw_block.get("results") or [])]
            credits = data.get("credits", {})
            cast = [c["name"] for c in (credits.get("cast") or [])[:5]]
            if media_type == "movie":
                directors = [c["name"] for c in (credits.get("crew") or [])
                             if c.get("job") == "Director"]
                runtime = data.get("runtime")
            else:
                directors = [c["name"] for c in (data.get("created_by") or [])]
                run_times = data.get("episode_run_time") or []
                runtime = run_times[0] if run_times else None
            conn.execute(
                "INSERT OR REPLACE INTO ai_title_metadata "
                "(tmdb_id, media_type, title, keywords, cast_names, directors, runtime, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (tmdb_id, media_type, media.get("title"), json.dumps(keywords),
                 json.dumps(cast), json.dumps(directors), runtime,
                 datetime.now(timezone.utc).isoformat()),
            )
            fetched += 1
    if fetched:
        conn.commit()
        log.info(f"Taste profile: cached TMDB metadata for {fetched} new titles")
    return fetched


def load_metadata(conn):
    """The full metadata cache as {(tmdb_id, media_type): {...}}."""
    ensure_metadata_table(conn)
    out = {}
    for tmdb_id, media_type, kw, cast, directors, runtime in conn.execute(
            "SELECT tmdb_id, media_type, keywords, cast_names, directors, runtime "
            "FROM ai_title_metadata"):
        out[(tmdb_id, media_type)] = {
            "keywords": json.loads(kw or "[]"),
            "cast": json.loads(cast or "[]"),
            "directors": json.loads(directors or "[]"),
            "runtime": runtime,
        }
    return out


# ============================================================
# JELLYFIN SIGNALS (one selected user)
# ============================================================
def _jellyfin_get(url, api_key, endpoint, params=None):
    resp = requests.get(f"{url}{endpoint}", params=params or {},
                        headers={"X-Emby-Token": api_key}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def resolve_jellyfin_user(url, api_key, name_or_id):
    """AI_PROFILE_JELLYFIN_USER accepts a display name (friendlier than the
    GUIDs in JELLYFIN_USER_IDS) or a raw GUID. Returns the user id, or None
    with a warning — an unresolvable user disables Jellyfin signals only."""
    if not name_or_id:
        return None
    if re.fullmatch(r"[0-9a-fA-F]{32}|[0-9a-fA-F-]{36}", name_or_id):
        return name_or_id
    try:
        users = _jellyfin_get(url, api_key, "/Users")
        for user in users:
            if (user.get("Name") or "").lower() == name_or_id.lower():
                return user.get("Id")
        log.warning(f"Taste profile: no Jellyfin user named '{name_or_id}' "
                    f"(have: {', '.join(u.get('Name', '?') for u in users)})")
    except Exception as e:
        log.warning(f"Taste profile: could not resolve Jellyfin user: {e}")
    return None


def fetch_jellyfin_signals(url, api_key, user_id):
    """Per-person implicit feedback the shared Trakt account can't provide:
    what THIS user finished fast, dropped, favorited, or rewatched.
    Returns {"movies": {title: {...}}, "series": {title: {...}}} or None."""
    if not user_id:
        return None
    try:
        movies = _jellyfin_get(url, api_key, f"/Users/{user_id}/Items", {
            "IncludeItemTypes": "Movie", "Recursive": "true",
            "Filters": "IsPlayed", "Fields": "UserData",
        }).get("Items", [])
        episodes = _jellyfin_get(url, api_key, f"/Users/{user_id}/Items", {
            "IncludeItemTypes": "Episode", "Recursive": "true",
            "Fields": "UserData,SeriesName",
        }).get("Items", [])
        # Show-level IsFavorite lives on the Series item — users favorite the
        # show, not individual episodes, so the episode list never sees it.
        show_items = _jellyfin_get(url, api_key, f"/Users/{user_id}/Items", {
            "IncludeItemTypes": "Series", "Recursive": "true",
            "Fields": "UserData",
        }).get("Items", [])
    except Exception as e:
        log.warning(f"Taste profile: Jellyfin signals unavailable: {e}")
        return None

    movie_stats = {}
    for m in movies:
        name = m.get("Name")
        if not name:  # a nameless item would poison the title lists downstream
            continue
        ud = m.get("UserData") or {}
        movie_stats[name] = {
            "plays": ud.get("PlayCount", 0),
            "favorite": bool(ud.get("IsFavorite")),
        }

    series = defaultdict(lambda: {"available": 0, "played": 0,
                                  "first": None, "last": None, "favorite": False})
    for ep in episodes:
        name = ep.get("SeriesName")
        if not name:
            continue
        s = series[name]
        s["available"] += 1
        ud = ep.get("UserData") or {}
        if ud.get("Played"):
            s["played"] += 1
            when = ud.get("LastPlayedDate")
            if when:
                s["first"] = min(s["first"] or when, when)
                s["last"] = max(s["last"] or when, when)
        if ud.get("IsFavorite"):
            s["favorite"] = True
    # Seed show-level favorites (the reliable signal; episode favorites above
    # are a weak bonus).
    for item in show_items:
        name = item.get("Name")
        if name and (item.get("UserData") or {}).get("IsFavorite"):
            series[name]["favorite"] = True
    return {"movies": movie_stats, "series": dict(series)}


# ============================================================
# STATISTICS (pure functions, stdlib only)
# ============================================================
def _decay_weight(last_watched, now, half_life_days):
    """0.5 ** (days_since / half_life): a watch half_life days ago counts
    half as much as one today. Unknown dates get a token weight so old
    imports still register without swamping recent taste."""
    if not last_watched:
        return 0.1
    try:
        watched_at = datetime.fromisoformat(last_watched.replace("Z", "+00:00"))
    except ValueError:
        return 0.1
    days = max((now - watched_at).total_seconds() / 86400, 0)
    return 0.5 ** (days / half_life_days)


def _parse_jf_date(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def compute_profile(watched, metadata, jf_signals, ratings=None, now=None,
                    half_life_days=90, baseline_genres=None):
    """All profile statistics from pre-fetched inputs. Pure — unit-testable
    with fixtures, no I/O. Returns a dict of ranked signal lists.
    baseline_genres: {genre: population_share} (e.g. from trending) used to
    compute lift — how much the user over-indexes on a genre vs. that
    baseline — so the profile can say what's DISTINCTIVE, not just frequent."""
    now = now or datetime.now(timezone.utc)

    genre_w = defaultdict(float)
    keyword_w = defaultdict(float)
    people_w = defaultdict(float)
    people_n = defaultdict(int)
    decade_w = defaultdict(float)
    runtime_pairs = {"show": [], "movie": []}  # (runtime, weight)
    rewatched = []
    total_w = 0.0
    total_n = 0

    for media_type in ("show", "movie"):
        for item in (watched.get(media_type) or []):
            media = item.get(media_type, {})
            title = media.get("title")
            if not title:
                continue
            w = _decay_weight(item.get("last_watched_at"), now, half_life_days)
            total_w += w
            total_n += 1
            for genre in media.get("genres") or []:
                # Trakt returns hyphenated slugs ("science-fiction"); normalize
                # to spaces so lookups against the majors set match and the
                # rendered names read naturally.
                genre_w[genre.lower().replace("-", " ")] += w
            year = media.get("year")
            if year:
                decade_w[f"{(year // 10) * 10}s"] += w
            if item.get("plays", 0) >= 2 and media_type == "movie":
                rewatched.append((item["plays"], title, year))
            meta = metadata.get(((media.get("ids") or {}).get("tmdb"), media_type))
            if meta:
                for kw in meta["keywords"]:
                    keyword_w[kw] += w
                for person in meta["cast"] + meta["directors"]:
                    people_w[person] += w
                    people_n[person] += 1
                if meta["runtime"]:
                    runtime_pairs[media_type].append((meta["runtime"], w))

    # Lift = user's share of a genre vs. the baseline population's share.
    # Laplace-smoothed so a genre absent from the (small) baseline sample
    # doesn't explode to a huge multiplier. None when no baseline supplied.
    user_genre_total = sum(genre_w.values()) or 1.0

    def _lift(g):
        if not baseline_genres:
            return None
        eps = 0.02
        u = genre_w[g] / user_genre_total
        return (u + eps) / (baseline_genres.get(g, 0.0) + eps)

    top_genres = sorted(genre_w.items(), key=lambda kv: -kv[1])[:6]
    genre_shares = ([(g, w / total_w, _lift(g)) for g, w in top_genres]
                    if total_w else [])
    # "Avoided" = major genres conspicuous by absence. Only genres common
    # enough that a zero share is a choice, not a coincidence.
    majors = {"drama", "comedy", "action", "thriller", "science fiction",
              "fantasy", "horror", "romance", "documentary", "animation",
              "crime", "mystery", "reality", "western", "war", "family"}
    avoided = sorted(g for g in majors
                     if genre_w.get(g, 0) / total_w < 0.01) if total_w else []

    def weighted_median(pairs):
        if not pairs:
            return None
        pairs.sort()
        half = sum(w for _, w in pairs) / 2
        acc = 0.0
        for value, w in pairs:
            acc += w
            if acc >= half:
                return value
        return pairs[-1][0]

    binged, favorites = [], []
    if jf_signals:
        favorites = [t for t, s in jf_signals["movies"].items() if s["favorite"]]
        favorites += [t for t, s in jf_signals["series"].items() if s["favorite"]]
        rewatched += [(s["plays"], t, None) for t, s in jf_signals["movies"].items()
                      if s["plays"] >= 2]
        for title, s in jf_signals["series"].items():
            if s["available"] < MIN_EPISODES_FOR_PACE or not s["played"]:
                continue
            completion = s["played"] / s["available"]
            first, last = _parse_jf_date(s["first"]), _parse_jf_date(s["last"])
            # Fast, near-complete watch = strong positive. Low completion is NOT
            # treated as a negative (see the note by BINGE_* — it's backlog).
            if (completion >= BINGE_COMPLETION and first and last
                    and (last - first) <= timedelta(days=BINGE_WINDOW_DAYS)):
                binged.append(title)

    loved_titles, disliked_titles = [], []
    if ratings and len(ratings) >= MIN_RATINGS_TO_USE:
        for entry in ratings:
            media = entry.get(entry.get("type"), {})
            title, score = media.get("title"), entry.get("rating")
            if not title or score is None:
                continue
            if score >= 9:
                loved_titles.append((score, title))
            elif score <= 5:
                disliked_titles.append((score, title))

    return {
        "sample_size": total_n,
        "genres": genre_shares,
        "avoided_genres": avoided,
        "keywords": sorted(keyword_w.items(), key=lambda kv: -kv[1])[:15],
        "people": sorted(((p, w) for p, w in people_w.items() if people_n[p] >= 2),
                         key=lambda kv: -kv[1])[:10],
        "decades": sorted(decade_w.items(), key=lambda kv: -kv[1])[:3],
        "runtime_movie": weighted_median(runtime_pairs["movie"]),
        "runtime_show": weighted_median(runtime_pairs["show"]),
        # Explicit key: entries carry year=None (Jellyfin, unyeared Trakt
        # titles), and a bare tuple sort would compare int < None on ties.
        "rewatched": [t for _, t, _ in
                      sorted(rewatched, key=lambda r: (-r[0], r[1]))[:10]],
        "favorites": favorites[:10],
        "binged": binged[:8],
        "loved": [t for _, t in sorted(loved_titles, reverse=True)[:10]],
        "disliked": [t for _, t in sorted(disliked_titles)[:8]],
    }


def render_profile(profile):
    """The prompt block. Compact prose-ish lines — Gemini does the semantic
    matching; this just has to be accurate and specific. Also written to
    ai_taste_profile.txt so the user can read exactly what is sent."""
    lines = [f"TASTE PROFILE (computed locally from {profile['sample_size']} "
             f"watched titles, recency-weighted — recent watching counts more):"]
    if profile["genres"]:
        parts = []
        for g, share, lift in profile["genres"]:
            # Surface lift only when it's meaningfully above baseline — that's
            # the "distinctive vs. the average viewer" signal worth flagging.
            if lift and lift >= 1.3:
                parts.append(f"{g} ({share:.0%}, {lift:.1f}× vs trending)")
            else:
                parts.append(f"{g} ({share:.0%})")
        lines.append("- Genres watched most: " + ", ".join(parts))
    if profile["avoided_genres"]:
        lines.append("- Genres essentially never watched (do not suggest): "
                     + ", ".join(profile["avoided_genres"]))
    if profile["keywords"]:
        lines.append("- Recurring themes: " + ", ".join(k for k, _ in profile["keywords"]))
    if profile["people"]:
        lines.append("- Actors/creators watched repeatedly: "
                     + ", ".join(p for p, _ in profile["people"]))
    if profile["decades"]:
        lines.append("- Release-era preference: "
                     + ", ".join(d for d, _ in profile["decades"]))
    runtimes = []
    if profile["runtime_movie"]:
        runtimes.append(f"movies ~{profile['runtime_movie']} min")
    if profile["runtime_show"]:
        runtimes.append(f"show episodes ~{profile['runtime_show']} min")
    if runtimes:
        lines.append("- Typical runtime: " + ", ".join(runtimes))
    if profile["rewatched"]:
        lines.append("- Rewatched (all-time favorites): " + ", ".join(profile["rewatched"]))
    if profile["favorites"]:
        lines.append("- Marked favorite on the media server: " + ", ".join(profile["favorites"]))
    if profile["binged"]:
        lines.append("- Binged fast (strong likes): " + ", ".join(profile["binged"]))
    if profile["loved"]:
        lines.append("- Personally rated 9-10: " + ", ".join(profile["loved"]))
    if profile["disliked"]:
        lines.append("- Personally rated 5 or below (avoid similar): "
                     + ", ".join(profile["disliked"]))
    return "\n".join(lines)


# TMDB genre_id -> canonical name aligned with Trakt's normalized vocabulary
# (lowercase, spaces). Covers both the movie and TV id sets; a couple of TV
# combos ("Action & Adventure", "Sci-Fi & Fantasy") fold to their closest
# Trakt genre so the baseline shares the user's vocabulary and lift lines up.
_TMDB_GENRE_CANON = {
    28: "action", 12: "adventure", 16: "animation", 35: "comedy",
    80: "crime", 99: "documentary", 18: "drama", 10751: "family",
    14: "fantasy", 36: "history", 27: "horror", 10402: "music",
    9648: "mystery", 10749: "romance", 878: "science fiction",
    53: "thriller", 10752: "war", 37: "western",
    10759: "action", 10762: "family", 10763: "news", 10764: "reality",
    10765: "science fiction", 10766: "soap", 10767: "talk show", 10768: "war",
}


def trending_genre_baseline(tmdb_get):
    """Genre proportion of this week's TMDB trending — the population Gemini
    draws from. Used as the lift baseline so the profile can flag what the
    user over-indexes on. Empty dict if TMDB is unavailable (lift then off)."""
    counts = defaultdict(float)
    for tmdb_type in ("movie", "tv"):
        data = tmdb_get(f"/trending/{tmdb_type}/week")
        for entry in (data or {}).get("results", []):
            for gid in entry.get("genre_ids") or []:
                name = _TMDB_GENRE_CANON.get(gid)
                if name:
                    counts[name] += 1
    total = sum(counts.values())
    return {g: c / total for g, c in counts.items()} if total else {}


# ============================================================
# ORCHESTRATOR
# ============================================================
def build_and_render(conn, watched, tmdb_get, trakt_get, jellyfin_url,
                     jellyfin_api_key, jellyfin_user, half_life_days=90,
                     max_tmdb_fetches=40):
    """One call from trakt_discovery: collect (incrementally), compute,
    render. Any failure returns None — the AI cycle proceeds profile-less,
    which is exactly the pre-v1.10.0 prompt."""
    try:
        collect_title_metadata(conn, watched, tmdb_get, max_fetches=max_tmdb_fetches)
        metadata = load_metadata(conn)

        jf_signals = None
        if jellyfin_url and jellyfin_api_key and jellyfin_user:
            user_id = resolve_jellyfin_user(jellyfin_url, jellyfin_api_key, jellyfin_user)
            jf_signals = fetch_jellyfin_signals(jellyfin_url, jellyfin_api_key, user_id)

        ratings = []
        for media_type in ("movies", "shows"):
            got = trakt_get(f"/users/me/ratings/{media_type}", auth_required=True, conn=conn)
            if isinstance(got, list):
                ratings.extend(got)

        profile = compute_profile(watched, metadata, jf_signals, ratings,
                                  half_life_days=half_life_days,
                                  baseline_genres=trending_genre_baseline(tmdb_get))
        if not profile["sample_size"]:
            return None
        return render_profile(profile)
    except Exception as e:
        log.warning(f"Taste profile unavailable this cycle: {e}")
        return None
