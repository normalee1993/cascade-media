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

from security_utils import redact, sanitize_prompt_text

log = logging.getLogger("taste_profile")

# Below this many personal ratings, the ratings section is skipped — a
# handful of ratings says more about forgetting to rate than about taste.
MIN_RATINGS_TO_USE = 10

# Series shorter than this many downloaded episodes can't meaningfully be
# "binged" — a 3-episode miniseries watched in one sitting says nothing that
# plain watch history doesn't.
MIN_EPISODES_FOR_PACE = 4

# "Essentially never watched" genres are only declared from a history big enough
# for absence to mean something; below this the avoided list stays empty (R4).
MIN_TITLES_FOR_AVOIDED = 30

# Stop topping up the TMDB cache for the cycle after this many lookups in a row
# fail (outage / rate limiting), instead of hammering TMDB title after title (R2).
MAX_CONSECUTIVE_TMDB_FAILURES = 3

# Jellyfin /Items page size — paging keeps a large library from being pulled
# into memory in one response (S32).
JELLYFIN_PAGE_SIZE = 500

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
    (successful or not) per cycle, newest watches first across both media types
    (they dominate the decayed stats anyway). Returns the number fetched.

    Failures never escape (R1): the real tmdb_get RAISES on 404 / timeouts /
    final-attempt 5xx. A 404 (stale or merged TMDB id) is stored as an empty
    negative-cache row so it is never retried; other failures skip the title, and
    MAX_CONSECUTIVE_TMDB_FAILURES in a row end the top-up for this cycle (R2)."""
    ensure_metadata_table(conn)
    cached = {(r[0], r[1]) for r in
              conn.execute("SELECT tmdb_id, media_type FROM ai_title_metadata")}

    pending = []
    for media_type in ("show", "movie"):
        for item in (watched.get(media_type) or []):
            media = item.get(media_type) or {}
            tmdb_id = (media.get("ids") or {}).get("tmdb")
            if tmdb_id and (tmdb_id, media_type) not in cached:
                pending.append((item.get("last_watched_at") or "", media_type, tmdb_id, media))
                cached.add((tmdb_id, media_type))  # dedupe repeats within this cycle
    pending.sort(key=lambda p: p[0], reverse=True)  # newest first, both types (R11)

    fetched = attempts = negative = consecutive_failures = 0
    for _, media_type, tmdb_id, media in pending:
        if attempts >= max_fetches:
            break
        if consecutive_failures >= MAX_CONSECUTIVE_TMDB_FAILURES:
            log.warning(f"Taste profile: {consecutive_failures} TMDB lookups failed in a "
                        f"row; resuming the metadata top-up next cycle")
            break
        attempts += 1
        endpoint = ("/tv" if media_type == "show" else "/movie") + f"/{tmdb_id}"
        try:
            data = tmdb_get(endpoint, params={"append_to_response": "credits,keywords"})
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 404:
                _store_metadata(conn, tmdb_id, media_type, media.get("title"), [], [], [], None)
                negative += 1
                continue
            consecutive_failures += 1
            log.debug(f"Taste profile: TMDB lookup failed for {endpoint}: {redact(e)}")
            continue
        if not data:
            consecutive_failures += 1
            continue
        consecutive_failures = 0
        # Keywords nest differently per type: movies under "keywords",
        # shows under "results".
        kw_block = data.get("keywords") or {}
        keywords = [str(k.get("name", "")).lower()
                    for k in (kw_block.get("keywords") or kw_block.get("results") or [])
                    if k.get("name")]
        credits = data.get("credits") or {}
        cast = [c["name"] for c in (credits.get("cast") or [])[:5] if c.get("name")]
        if media_type == "movie":
            directors = [c["name"] for c in (credits.get("crew") or [])
                         if c.get("job") == "Director" and c.get("name")]
            runtime = data.get("runtime")
        else:
            directors = [c["name"] for c in (data.get("created_by") or []) if c.get("name")]
            run_times = data.get("episode_run_time") or []
            runtime = run_times[0] if run_times else None
        _store_metadata(conn, tmdb_id, media_type, media.get("title"),
                        keywords, cast, directors, runtime)
        fetched += 1
    if fetched or negative:
        conn.commit()
        log.info(f"Taste profile: cached TMDB metadata for {fetched} new titles"
                 + (f" ({negative} not on TMDB)" if negative else ""))
    return fetched


def _store_metadata(conn, tmdb_id, media_type, title, keywords, cast, directors, runtime):
    conn.execute(
        "INSERT OR REPLACE INTO ai_title_metadata "
        "(tmdb_id, media_type, title, keywords, cast_names, directors, runtime, collected_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (tmdb_id, media_type, title, json.dumps(keywords), json.dumps(cast),
         json.dumps(directors), runtime, datetime.now(timezone.utc).isoformat()),
    )


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
def _jellyfin_get(url, api_key, endpoint, params=None, attempts=2):
    """GET a Jellyfin endpoint. One retry on a transient failure; redirects are
    not followed (they would carry X-Emby-Token to another host); errors are
    re-raised with credentials redacted."""
    last = None
    for attempt in range(attempts):
        try:
            resp = requests.get(f"{url.rstrip('/')}{endpoint}", params=params or {},
                                headers={"X-Emby-Token": api_key}, timeout=60,
                                allow_redirects=False)
            if 300 <= resp.status_code < 400:
                raise requests.exceptions.RequestException(
                    f"Jellyfin redirect ({resp.status_code}) refused — set JELLYFIN_URL to the final address")
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last = e
            continue
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(redact(e)) from None
    raise requests.exceptions.RequestException(redact(last)) from None


def _jellyfin_paged(url, api_key, endpoint, params, page_size=None, max_items=200_000):
    """Yield every item of a Jellyfin /Items query, one page at a time, so a big
    library is never held in memory as a single response (S32)."""
    page_size = page_size or JELLYFIN_PAGE_SIZE
    start = 0
    while start < max_items:
        page = _jellyfin_get(url, api_key, endpoint, {
            **params, "StartIndex": str(start), "Limit": str(page_size),
            "EnableImages": "false", "EnableTotalRecordCount": "true",
        }) or {}
        items = page.get("Items") or []
        yield from items
        start += len(items)
        total = page.get("TotalRecordCount")
        if len(items) < page_size or (isinstance(total, int) and start >= total):
            break


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
                    f"(have: {', '.join(str(u.get('Name') or '?') for u in users)})")
    except Exception as e:
        log.warning(f"Taste profile: could not resolve Jellyfin user: {redact(e)}")
    return None


def fetch_jellyfin_signals(url, api_key, user_id):
    """Per-person implicit feedback the shared Trakt account can't provide:
    what THIS user finished fast, favorited, or rewatched.
    Returns {"movies": {title: {...}}, "series": {title: {...}}} or None.

    Paged and limited to played items (S32): episode counts per series come from
    the Series items' UnplayedItemCount, so the whole episode library is never
    downloaded just to count it."""
    if not user_id:
        return None
    items_ep = f"/Users/{user_id}/Items"
    movie_stats = {}
    series = defaultdict(lambda: {"available": 0, "played": 0, "unplayed": None,
                                  "first": None, "last": None, "favorite": False})
    try:
        for m in _jellyfin_paged(url, api_key, items_ep, {
                "IncludeItemTypes": "Movie", "Recursive": "true", "Filters": "IsPlayed"}):
            name = m.get("Name")
            if not name:  # a nameless item would poison the title lists downstream
                continue
            ud = m.get("UserData") or {}
            movie_stats[name] = {
                "plays": ud.get("PlayCount") or 0,
                "favorite": bool(ud.get("IsFavorite")),
                "last": ud.get("LastPlayedDate"),
            }

        for ep in _jellyfin_paged(url, api_key, items_ep, {
                "IncludeItemTypes": "Episode", "Recursive": "true", "Filters": "IsPlayed",
                "Fields": "SeriesName"}):
            name = ep.get("SeriesName")
            if not name:
                continue
            s = series[name]
            ud = ep.get("UserData") or {}
            if ud.get("Played", True):
                s["played"] += 1
                when = ud.get("LastPlayedDate")
                if when:
                    s["first"] = min(s["first"] or when, when)
                    s["last"] = max(s["last"] or when, when)
            if ud.get("IsFavorite"):
                s["favorite"] = True

        # Show-level IsFavorite + unplayed counts live on the Series item — users
        # favorite the show, not individual episodes.
        for item in _jellyfin_paged(url, api_key, items_ep, {
                "IncludeItemTypes": "Series", "Recursive": "true"}):
            name = item.get("Name")
            if not name:
                continue
            ud = item.get("UserData") or {}
            if ud.get("IsFavorite"):
                series[name]["favorite"] = True
            if isinstance(ud.get("UnplayedItemCount"), int) and name in series:
                series[name]["unplayed"] = ud["UnplayedItemCount"]
    except Exception as e:
        log.warning(f"Taste profile: Jellyfin signals unavailable: {redact(e)}")
        return None

    for s in series.values():
        # available = played + unplayed. Unknown unplayed count => available 0,
        # which keeps the series out of binge detection rather than guessing.
        unplayed = s.pop("unplayed", None)
        s["available"] = s["played"] + unplayed if unplayed is not None else 0
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
    except (AttributeError, ValueError):
        return 0.1
    if watched_at.tzinfo is None:
        watched_at = watched_at.replace(tzinfo=timezone.utc)
    days = max((now - watched_at).total_seconds() / 86400, 0)
    return 0.5 ** (days / max(half_life_days, 1))


def _parse_jf_date(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def compute_profile(watched, metadata, jf_signals, ratings=None, now=None,
                    half_life_days=90, baseline_genres=None):
    """All profile statistics from pre-fetched inputs. Pure — unit-testable
    with fixtures, no I/O. Returns a dict of ranked signal lists.
    baseline_genres: {genre: population_share} (e.g. from trending) used to
    compute lift — how much the user over-indexes on a genre vs. that
    baseline — so the profile can say what's DISTINCTIVE, not just frequent."""
    now = now or datetime.now(timezone.utc)

    genre_w = defaultdict(float)
    genre_n = defaultdict(int)   # undecayed title counts, for "never watched" (R3)
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
            for genre in {g.lower().replace("-", " ") for g in (media.get("genres") or [])}:
                # Trakt returns hyphenated slugs ("science-fiction"); normalize
                # to spaces so lookups against the majors set match and the
                # rendered names read naturally.
                genre_w[genre] += w
                genre_n[genre] += 1
            year = media.get("year")
            if year:
                decade_w[f"{(year // 10) * 10}s"] += w
            if (item.get("plays") or 0) >= 2 and media_type == "movie":
                rewatched.append((item["plays"], title, year))
            meta = metadata.get(((media.get("ids") or {}).get("tmdb"), media_type))
            if meta:
                for kw in meta["keywords"]:
                    keyword_w[kw] += w
                # A person who both stars in and created/directed a title counts
                # once for it — "watched repeatedly" means >= 2 titles (R8).
                for person in set(meta["cast"]) | set(meta["directors"]):
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
    # enough that a zero share is a choice, not a coincidence. Judged on
    # UNdecayed counts over the whole history (a genre binged two years ago is
    # not "never watched" — R3), and only once the history is big enough for
    # absence to mean anything (R4).
    majors = {"drama", "comedy", "action", "thriller", "science fiction",
              "fantasy", "horror", "romance", "documentary", "animation",
              "crime", "mystery", "reality", "western", "war", "family"}
    avoided = (sorted(g for g in majors if genre_n.get(g, 0) / total_n < 0.01)
               if total_n >= MIN_TITLES_FOR_AVOIDED else [])

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
        # Rank by most recent play, not API (alphabetical) order, and mix movies
        # and series so neither crowds the other out of the top slots (R10).
        fav = [(s.get("last") or "", t) for t, s in jf_signals["movies"].items() if s["favorite"]]
        fav += [(s.get("last") or "", t) for t, s in jf_signals["series"].items() if s["favorite"]]
        favorites = [t for _, t in sorted(fav, key=lambda x: x[0], reverse=True)]
        rewatched += [(s["plays"], t, None) for t, s in jf_signals["movies"].items()
                      if (s.get("plays") or 0) >= 2]
        for title, s in jf_signals["series"].items():
            if s["available"] < MIN_EPISODES_FOR_PACE or not s["played"]:
                continue
            completion = s["played"] / s["available"]
            first, last = _parse_jf_date(s["first"]), _parse_jf_date(s["last"])
            # Fast, near-complete watch = strong positive. Low completion is NOT
            # treated as a negative (see the note by BINGE_* — it's backlog).
            if (completion >= BINGE_COMPLETION and first and last
                    and (last - first) <= timedelta(days=BINGE_WINDOW_DAYS)):
                binged.append((last, title))
        binged = [t for _, t in sorted(binged, key=lambda x: x[0], reverse=True)]

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
        "rewatched": _dedupe_rewatched(rewatched)[:10],
        "favorites": favorites[:10],
        "binged": binged[:8],
        "loved": [t for _, t in sorted(loved_titles, reverse=True)[:10]],
        "disliked": [t for _, t in sorted(disliked_titles)[:8]],
    }


def _dedupe_rewatched(entries):
    """Rewatches arrive from Trakt AND Jellyfin (Trakt is scrobbled from it), so
    the same movie appears twice. Keep one entry per normalized title with the
    highest play count, ranked by plays (R7)."""
    best = {}
    for plays, title, _year in entries:
        key = re.sub(r"[^a-z0-9]+", "", str(title).lower())
        if key not in best or plays > best[key][0]:
            best[key] = (plays, title)
    return [t for _, t in sorted(best.values(), key=lambda r: (-r[0], r[1]))]


def render_profile(profile):
    """The prompt block. Compact prose-ish lines — Gemini does the semantic
    matching; this just has to be accurate and specific. Also written to
    ai_taste_profile.txt so the user can read exactly what is sent."""
    # Keywords, names and titles come from TMDB/Jellyfin/Trakt: flatten each to a
    # bounded single line so none can inject prompt structure (S26).
    def j(values):
        return ", ".join(sanitize_prompt_text(v, 80) for v in values)

    lines = [f"TASTE PROFILE (computed locally from {profile['sample_size']} "
             f"watched titles, recency-weighted — recent watching counts more):"]
    if profile["genres"]:
        parts = []
        for g, share, lift in profile["genres"]:
            # Surface lift only when it's meaningfully above baseline — that's
            # the "distinctive vs. the average viewer" signal worth flagging.
            if lift and lift >= 1.3:
                parts.append(f"{sanitize_prompt_text(g, 40)} ({share:.0%}, {lift:.1f}× vs trending)")
            else:
                parts.append(f"{sanitize_prompt_text(g, 40)} ({share:.0%})")
        lines.append("- Genres watched most: " + ", ".join(parts))
    if profile["avoided_genres"]:
        lines.append("- Genres essentially never watched (do not suggest): "
                     + j(profile["avoided_genres"]))
    if profile["keywords"]:
        lines.append("- Recurring themes: " + j(k for k, _ in profile["keywords"]))
    if profile["people"]:
        lines.append("- Actors/creators watched repeatedly: "
                     + j(p for p, _ in profile["people"]))
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
        lines.append("- Rewatched (all-time favorites): " + j(profile["rewatched"]))
    if profile["favorites"]:
        lines.append("- Marked favorite on the media server: " + j(profile["favorites"]))
    if profile["binged"]:
        lines.append("- Binged fast (strong likes): " + j(profile["binged"]))
    if profile["loved"]:
        lines.append("- Personally rated 9-10: " + j(profile["loved"]))
    if profile["disliked"]:
        lines.append("- Personally rated 5 or below (avoid similar): "
                     + j(profile["disliked"]))
    return "\n".join(lines)


# TMDB genre_id -> canonical name(s) aligned with Trakt's normalized vocabulary
# (lowercase, spaces). Covers both the movie and TV id sets. TV combo genres
# ("Action & Adventure", "Sci-Fi & Fantasy", "War & Politics") count toward
# BOTH halves, because Trakt tags such shows with both genres — folding them to
# one side under-counted the other in the baseline and inflated its lift (R9).
_TMDB_GENRE_CANON = {
    28: ("action",), 12: ("adventure",), 16: ("animation",), 35: ("comedy",),
    80: ("crime",), 99: ("documentary",), 18: ("drama",), 10751: ("family",),
    14: ("fantasy",), 36: ("history",), 27: ("horror",), 10402: ("music",),
    9648: ("mystery",), 10749: ("romance",), 878: ("science fiction",),
    53: ("thriller",), 10752: ("war",), 37: ("western",),
    10759: ("action", "adventure"), 10762: ("family",), 10763: ("news",),
    10764: ("reality",), 10765: ("science fiction", "fantasy"), 10766: ("soap",),
    10767: ("talk show",), 10768: ("war", "politics"),
}


def trending_genre_baseline(tmdb_get=None, trending_raw=None):
    """Genre proportion of this week's TMDB trending — the population Gemini
    draws from. Used as the lift baseline so the profile can flag what the
    user over-indexes on. Empty dict if TMDB is unavailable (lift then off).

    trending_raw: the {"tv": ..., "movie": ...} responses trakt_discovery
    already fetched for the prompt, so the same endpoints aren't hit twice per
    cycle (R14). Falls back to fetching via tmdb_get when not supplied."""
    counts = defaultdict(float)
    for tmdb_type in ("movie", "tv"):
        if trending_raw is not None:
            data = trending_raw.get(tmdb_type)
        else:
            data = tmdb_get(f"/trending/{tmdb_type}/week") if tmdb_get else None
        for entry in (data or {}).get("results", []):
            for gid in entry.get("genre_ids") or []:
                for name in _TMDB_GENRE_CANON.get(gid, ()):
                    counts[name] += 1
    total = sum(counts.values())
    return {g: c / total for g, c in counts.items()} if total else {}


# ============================================================
# ORCHESTRATOR
# ============================================================
def build_and_render(conn, watched, tmdb_get, trakt_get, jellyfin_url,
                     jellyfin_api_key, jellyfin_user, half_life_days=90,
                     max_tmdb_fetches=40, trending_raw=None):
    """One call from trakt_discovery: collect (incrementally), compute,
    render. Any failure returns None — the AI cycle proceeds profile-less,
    which is exactly the pre-v1.10.0 prompt. Optional inputs (Jellyfin, ratings,
    the lift baseline) degrade individually instead of sinking the profile (R5)."""
    try:
        half_life_days = max(1, int(half_life_days or 90))  # 0 would divide by zero (R6)
        collect_title_metadata(conn, watched, tmdb_get, max_fetches=max_tmdb_fetches)
        metadata = load_metadata(conn)

        jf_signals = None
        if jellyfin_url and jellyfin_api_key and jellyfin_user:
            user_id = resolve_jellyfin_user(jellyfin_url, jellyfin_api_key, jellyfin_user)
            jf_signals = fetch_jellyfin_signals(jellyfin_url, jellyfin_api_key, user_id)

        ratings = []
        for media_type in ("movies", "shows"):
            try:
                got = trakt_get(f"/users/me/ratings/{media_type}", auth_required=True, conn=conn)
            except Exception as e:
                log.warning(f"Taste profile: Trakt {media_type} ratings unavailable: {redact(e)}")
                continue
            if isinstance(got, list):
                ratings.extend(got)

        try:
            baseline = trending_genre_baseline(tmdb_get, trending_raw=trending_raw)
        except Exception as e:
            log.warning(f"Taste profile: trending baseline unavailable (lift off): {redact(e)}")
            baseline = {}

        profile = compute_profile(watched, metadata, jf_signals, ratings,
                                  half_life_days=half_life_days,
                                  baseline_genres=baseline)
        if not profile["sample_size"]:
            return None
        return render_profile(profile)
    except Exception as e:
        log.warning(f"Taste profile unavailable this cycle: {redact(e)}")
        return None
