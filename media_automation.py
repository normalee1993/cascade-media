#!/usr/bin/env python3
"""
Media Automation for Unraid
- Sets new TV shows to: Requested season full + Episode 1 of all other seasons
- Monitors Jellyfin watch progress and auto-downloads next season at 75%
- Queries Seerr to determine which season was actually requested
- Runs on a configurable schedule
"""

import requests
import json
import time
import os
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

# ============================================================
# LOGGING (must be before config so log is available)
# ============================================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO"), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S"
)
log = logging.getLogger("media-automation")

# ============================================================
# CONFIGURATION (overridable via environment variables)
# ============================================================
def get_float_env(key, default):
    """Get float from environment with validation."""
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        log.error(f"Invalid {key}='{os.getenv(key)}', must be a number. Using default: {default}")
        return default

def get_int_env(key, default):
    """Get integer from environment with validation."""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        log.error(f"Invalid {key}='{os.getenv(key)}', must be an integer. Using default: {default}")
        return default

SONARR_URL = os.getenv("SONARR_URL", "")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", "")

# Radarr (movies). No client existed before Phase 3; mirrors the Sonarr config.
RADARR_URL = os.getenv("RADARR_URL", "")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "")

# Phase 3: auto-resolve "matched by ID" import blocks in Sonarr/Radarr. Master
# toggle; default on whenever the relevant *_API_KEY is configured. Set to
# "false" to disable even when keys are present.
AUTO_IMPORT_BLOCKED = os.getenv("AUTO_IMPORT_BLOCKED", "true").lower() == "true"

JELLYFIN_URL = os.getenv("JELLYFIN_URL", "")
JELLYFIN_API_KEY = os.getenv("JELLYFIN_API_KEY", "")

SEERR_URL = os.getenv("SEERR_URL", "")
SEERR_API_KEY = os.getenv("SEERR_API_KEY", "")

# Watch progress threshold (0.75 = 75% of season watched triggers next season download)
WATCH_THRESHOLD = get_float_env("WATCH_THRESHOLD", 0.75)
if not 0 < WATCH_THRESHOLD <= 1:
    log.warning(f"WATCH_THRESHOLD={WATCH_THRESHOLD} is out of range (0-1), using 0.75")
    WATCH_THRESHOLD = 0.75

# How far back to look for newly added series (in hours)
NEW_SERIES_LOOKBACK_HOURS = get_int_env("NEW_SERIES_LOOKBACK_HOURS", 24)

# A processed_series row stuck in 'in_progress' beyond this many minutes is
# considered abandoned (the owning subprocess crashed or was killed mid-setup)
# and may be re-claimed by a later run so the series self-heals instead of
# staying permanently half-configured. See claim_series_for_processing().
STALE_CLAIM_MINUTES = get_int_env("STALE_CLAIM_MINUTES", 30)

# Jellyfin user IDs to monitor (exclude SuggestArr bot)
JELLYFIN_USER_IDS = os.getenv("JELLYFIN_USER_IDS", "").split(",")
JELLYFIN_USER_IDS = [uid.strip() for uid in JELLYFIN_USER_IDS if uid.strip()]

SABNZBD_URL = os.getenv("SABNZBD_URL", "")
SABNZBD_API_KEY = os.getenv("SABNZBD_API_KEY", "")
SABNZBD_QUEUE_WAIT_SECONDS = get_int_env("SABNZBD_QUEUE_WAIT_SECONDS", 120)

# Phase 1: in-progress weekly priority boost. A season the user is actively
# working through (>=1 episode played, most-recent play within this many days)
# gets its still-queued episodes bumped to High priority so the next episode
# lands before the user catches up. 0/negative effectively disables the boost.
INPROGRESS_BOOST_WINDOW_DAYS = get_int_env("INPROGRESS_BOOST_WINDOW_DAYS", 7)

# Database path for tracking what we've already processed
DB_PATH = os.getenv("DB_PATH", "/data/media_automation.db")

# Dry run mode
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ============================================================
# API HELPERS
# ============================================================
SONARR_HEADERS = {"X-Api-Key": SONARR_API_KEY, "Content-Type": "application/json"}
RADARR_HEADERS = {"X-Api-Key": RADARR_API_KEY, "Content-Type": "application/json"}
JELLYFIN_HEADERS = {"X-Emby-Token": JELLYFIN_API_KEY, "Content-Type": "application/json"}
SEERR_HEADERS = {"X-Api-Key": SEERR_API_KEY, "Content-Type": "application/json"}


def _parse_retry_after(headers, default=60):
    """Parse a Retry-After header into seconds.

    Per RFC 7231 the header may be either a non-negative integer (delay seconds)
    or an HTTP-date. int() chokes on the date form and would crash the whole
    retry loop, so fall back to the default for anything non-numeric.
    """
    raw = headers.get('Retry-After', default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.warning(f"Non-numeric Retry-After header '{raw}', using {default}s")
        return default


def _session_equivalent(method, session):
    """Map a module-level requests.<verb> callable to the same verb bound to a Session.

    The public helpers pass requests.get/post/put/delete as `method`. When the
    in-process playback loop supplies its own thread-confined Session, we want the
    request to go through session.get/post/... (keep-alive reuse) instead of the
    module-level function. We resolve the verb by identity against the known
    requests functions; anything we don't recognise (e.g. a test MagicMock) is
    returned unchanged so behavior is preserved.
    """
    if session is None:
        return method
    verb = {
        requests.get: "get",
        requests.post: "post",
        requests.put: "put",
        requests.delete: "delete",
    }.get(method)
    if verb is None:
        return method
    return getattr(session, verb)


def _api_request_with_retry(method, url, headers, max_retries=3, session=None, **kwargs):
    """Make API request with retry logic for transient failures.

    When a `session` (requests.Session) is supplied, the call is routed through
    that session's bound verb (keep-alive connection reuse) instead of the
    module-level requests function. The session is owned by ONE caller thread
    (the in-process playback loop). When session is None this is a no-op and the
    original module-level `requests` callable is used, preserving behavior for
    every other (subprocess) entrypoint.
    """
    http_call = _session_equivalent(method, session)
    for attempt in range(max_retries):
        try:
            resp = http_call(url, headers=headers, timeout=30, **kwargs)

            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp.headers, 60)
                log.warning(f"Rate limited, waiting {retry_after}s before retry")
                time.sleep(retry_after)
                continue

            # Handle server errors with retry
            if resp.status_code >= 500:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    log.warning(f"Server error {resp.status_code}, retrying in {wait_time}s")
                    time.sleep(wait_time)
                    continue

            resp.raise_for_status()

            try:
                return resp.json()
            except json.JSONDecodeError:
                log.error(f"Invalid JSON response from {url}: {resp.text[:200]}")
                return None

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                log.warning(f"Request timeout, retrying ({attempt + 1}/{max_retries})")
                continue
            log.error(f"Request timed out after {max_retries} attempts: {url}")
            raise
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                log.warning(f"Connection error, retrying in {wait_time}s ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            log.error(f"Connection failed after {max_retries} attempts: {url}")
            raise
        except requests.exceptions.RequestException as e:
            log.error(f"API request failed: {url} - {e}")
            raise

    return None


def sonarr_get(endpoint, session=None):
    """GET request to Sonarr API."""
    return _api_request_with_retry(requests.get, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS, session=session)


def sonarr_put(endpoint, data, session=None):
    """PUT request to Sonarr API."""
    return _api_request_with_retry(requests.put, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS, session=session, json=data)


def sonarr_post(endpoint, data, session=None):
    """POST request to Sonarr API."""
    return _api_request_with_retry(requests.post, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS, session=session, json=data)


def radarr_get(endpoint, session=None):
    """GET request to Radarr API."""
    return _api_request_with_retry(requests.get, f"{RADARR_URL}/api/v3{endpoint}", RADARR_HEADERS, session=session)


def radarr_post(endpoint, data, session=None):
    """POST request to Radarr API."""
    return _api_request_with_retry(requests.post, f"{RADARR_URL}/api/v3{endpoint}", RADARR_HEADERS, session=session, json=data)


def sonarr_delete(endpoint):
    """DELETE request to Sonarr API."""
    url = f"{SONARR_URL}/api/v3{endpoint}"
    for attempt in range(3):
        try:
            resp = requests.delete(url, headers=SONARR_HEADERS, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
            log.error(f"DELETE request failed: {url} - {e}")
            raise
    return None


def jellyfin_get(endpoint, params=None, session=None):
    """GET request to Jellyfin API."""
    return _api_request_with_retry(requests.get, f"{JELLYFIN_URL}{endpoint}", JELLYFIN_HEADERS, session=session, params=params)


def seerr_get(endpoint, params=None):
    """GET request to Seerr API."""
    return _api_request_with_retry(requests.get, f"{SEERR_URL}/api/v1{endpoint}", SEERR_HEADERS, params=params)


# ============================================================
# SABNZBD API HELPERS
# ============================================================
def sabnzbd_api(mode, params=None, session=None):
    """Generic SABnzbd API call.

    When `session` is supplied (the in-process playback loop's thread-confined
    requests.Session) the GET reuses that session's keep-alive connection; else
    it falls back to the module-level requests.get (subprocess back-compat).
    """
    if not SABNZBD_API_KEY:
        log.warning("SABNZBD_API_KEY not set, skipping SABnzbd call")
        return None

    url = f"{SABNZBD_URL}/api"
    req_params = {"apikey": SABNZBD_API_KEY, "mode": mode, "output": "json"}
    if params:
        req_params.update(params)

    getter = session.get if session is not None else requests.get
    try:
        resp = getter(url, params=req_params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        # The exception string can embed the full request URL, which includes
        # ?apikey=<SABNZBD_API_KEY> as a query param. Logging it verbatim leaks
        # the key into `docker logs`. Log only the exception type and the
        # query-stripped base URL instead.
        log.error(f"SABnzbd API error (mode={mode}): {type(e).__name__} on {url}")
        return None


def sabnzbd_get_queue(session=None):
    """Get current SABnzbd queue slots."""
    data = sabnzbd_api("queue", session=session)
    if data and "queue" in data:
        return data["queue"].get("slots", [])
    return []


def sabnzbd_set_priority(nzo_id, priority, session=None):
    """Set priority for a SABnzbd queue item.
    Priority codes: -1=low, 0=normal, 1=high, 2=force
    """
    result = sabnzbd_api("queue", {"name": "priority", "value": nzo_id, "value2": str(priority)}, session=session)
    return result is not None


# ============================================================
# SEERR INTEGRATION
# ============================================================
def get_requested_seasons_from_seerr(tvdb_id, title):
    """Query Seerr to find which seasons were actually requested for a series."""
    if not SEERR_API_KEY:
        log.warning("  SEERR_API_KEY not set, cannot determine requested seasons")
        return None

    try:
        # Search Seerr requests (most recent first)
        request_data = seerr_get("/request", params={
            "take": 50,
            "skip": 0,
            "sort": "added",
        })

        if not request_data or "results" not in request_data:
            log.warning("  No request data from Seerr")
            return None

        # Find matching request by TVDB ID
        for req in request_data["results"]:
            if req.get("type") != "tv":
                continue

            media = req.get("media", {})
            req_tvdb = media.get("tvdbId")

            if req_tvdb and int(req_tvdb) == int(tvdb_id):
                # Found matching request - extract requested seasons
                requested_seasons = set()
                for season in req.get("seasons", []):
                    sn = season.get("seasonNumber", 0)
                    if sn > 0:
                        requested_seasons.add(sn)

                if requested_seasons:
                    log.info(f"  Seerr: Found request for '{title}' - seasons {sorted(requested_seasons)}")
                    return requested_seasons
                else:
                    # Request exists but no specific seasons listed (e.g., "remaining seasons")
                    log.info(f"  Seerr: Found request for '{title}' but no specific seasons listed (treating as 'all remaining')")
                    return set()  # Empty set means "request exists, no specific seasons"

        # Also try matching by title if TVDB didn't match
        for req in request_data["results"]:
            if req.get("type") != "tv":
                continue

            media = req.get("media", {})
            # Try to get title from the media info
            media_info = media.get("mediaInfo", {})
            req_title = media.get("title", "") or media_info.get("title", "")

            if req_title and req_title.lower().strip() == title.lower().strip():
                requested_seasons = set()
                for season in req.get("seasons", []):
                    sn = season.get("seasonNumber", 0)
                    if sn > 0:
                        requested_seasons.add(sn)

                if requested_seasons:
                    log.info(f"  Seerr: Found request for '{title}' (title match) - seasons {sorted(requested_seasons)}")
                    return requested_seasons

        log.info(f"  Seerr: No matching request found for '{title}' (tvdbId={tvdb_id})")
        return None

    except Exception as e:
        log.warning(f"  Seerr query failed: {e}")
        return None


# ============================================================
# DATABASE (tracks processed series and season unlocks)
# ============================================================
def _ensure_status_column(c):
    """Add processed_series.status if missing — idempotent AND safe under concurrent init_db.

    The scheduler launches the poll and playback subprocesses at the same instant and
    each calls init_db on the same database. Both can read PRAGMA table_info (seeing no
    `status` column) before either runs the ALTER, then serialize on the write — so the
    loser's `ALTER TABLE ... ADD COLUMN` raises "duplicate column name: status". That
    collision is benign (the column exists either way), so swallow it; re-raise anything
    else. Rows predating the column are backfilled to 'done' — they were fully set up
    under the old model and must never be re-processed. (Prod incident 2026-06-15.)
    """
    existing_cols = {row[1] for row in c.execute("PRAGMA table_info(processed_series)")}
    if "status" in existing_cols:
        return
    try:
        c.execute("ALTER TABLE processed_series ADD COLUMN status TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise
    c.execute("UPDATE processed_series SET status = 'done' WHERE status IS NULL")


def init_db():
    """Initialize SQLite database for tracking."""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # Retry logic to handle concurrent initialization
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
            # Set busy_timeout BEFORE journal_mode to handle concurrent access
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA journal_mode=WAL")

            c = conn.cursor()
            # ---- processed_series schema + status migration (Item 2) -------------
            # Fresh installs get the `status` column up front. status is one of
            # 'in_progress' (a subprocess has claimed the series and is applying
            # monitoring / running searches) or 'done' (setup fully completed).
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_series (
                    sonarr_id INTEGER PRIMARY KEY,
                    title TEXT,
                    processed_at TEXT,
                    status TEXT NOT NULL DEFAULT 'done'
                )
            """)
            # Idempotent migration for EXISTING populated DBs created before the
            # status column existed. Safe under concurrent init_db (the poll and
            # playback subprocesses race it at startup) — see _ensure_status_column.
            _ensure_status_column(c)
            # ---- end processed_series schema + status migration ------------------
            c.execute("""
                CREATE TABLE IF NOT EXISTS unlocked_seasons (
                    sonarr_id INTEGER,
                    season_number INTEGER,
                    unlocked_by TEXT,
                    unlocked_at TEXT,
                    PRIMARY KEY (sonarr_id, season_number)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS priority_boosts (
                    sonarr_id INTEGER,
                    season_number INTEGER,
                    boosted_at TEXT,
                    PRIMARY KEY (sonarr_id, season_number)
                )
            """)
            # ---- episode_boosts ledger (Phase 1: in-progress weekly boost) -------
            # Per-episode boost ledger, distinct from the season-level priority_boosts
            # table. Records that an individual queued episode of an in-progress
            # season has already been bumped to High priority so a later cycle does
            # not re-boost (and re-log) the same nzo.
            c.execute("""
                CREATE TABLE IF NOT EXISTS episode_boosts (
                    sonarr_id INTEGER,
                    season_number INTEGER,
                    episode_number INTEGER,
                    boosted_at TEXT,
                    PRIMARY KEY (sonarr_id, season_number, episode_number)
                )
            """)
            conn.commit()
            return conn
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.5 * (attempt + 1)  # Exponential backoff: 0.5s, 1s, 1.5s, 2s
                log.warning(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                log.error(f"Failed to initialize database after {max_retries} attempts: {e}")
                raise

    raise sqlite3.OperationalError("Failed to initialize database after all retries")


def check_db_writable(conn):
    """Probe DB writability before doing work that depends on persistence.

    Mirrors trakt_discovery.check_db_writable but is intentionally self-contained
    (media_automation has no alert infra, and must not import trakt_discovery).

    On a bind-mounted DB that is root-owned while the container runs as uid 99,
    SQLite opens the file read-only. init_db's CREATE TABLE IF NOT EXISTS would
    then crash every cycle with a cryptic "attempt to write a readonly database".
    This probe surfaces the readonly state loudly and lets the run skip cleanly.

    Returns True if writable, False (after logging an actionable message) if not.
    """
    try:
        # PRAGMA user_version = X writes the DB header even when X is unchanged,
        # reproducing the exact readonly error path without mutating any data.
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        conn.execute(f"PRAGMA user_version = {int(current)}")
        return True
    except sqlite3.OperationalError as e:
        log.error(
            f"Database is READONLY: {e}\n"
            "Monitoring/unlock/boost state cannot be persisted; skipping run.\n"
            "Fix on the Unraid host:\n"
            "  chown 99:users /mnt/user/appdata/media-automation/data/media_automation.db\n"
            "  chmod 664 /mnt/user/appdata/media-automation/data/media_automation.db"
        )
        return False


def is_series_processed(conn, sonarr_id):
    """Return True only if this series is FULLY set up (status='done').

    A series with an outstanding 'in_progress' claim is deliberately reported as
    NOT processed so that a later run (after the stale threshold) can re-claim
    and finish it. Callers that gate "skip, already done" therefore correctly
    skip only fully-completed series and retry crashed/half-done ones.
    """
    conn.commit()  # Ensure we read the latest cross-process committed state (WAL).
    c = conn.cursor()
    c.execute("SELECT 1 FROM processed_series WHERE sonarr_id = ? AND status = 'done'", (sonarr_id,))
    return c.fetchone() is not None


def claim_series_for_processing(conn, sonarr_id, title):
    """Atomically claim a series for setup. Returns True iff THIS process won.

    The claim is a single INSERT ... ON CONFLICT DO NOTHING, which is atomic
    across the separate OS subprocesses (poll, webhook, catchup) that all open
    their own WAL connection on the same DB. The winner is the process whose
    INSERT actually created the row (cursor.rowcount == 1); every other process
    sees rowcount == 0 and must NOT redo setup.

    Recovery: if the existing row is 'in_progress' but older than
    STALE_CLAIM_MINUTES, the owning process is presumed dead, so we forcibly
    take over the claim (refresh processed_at + re-stamp 'in_progress') and
    return True. If the existing row is 'done', or 'in_progress' and still
    fresh, we return False.
    """
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    with conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO processed_series (sonarr_id, title, status, processed_at)
            VALUES (?, ?, 'in_progress', ?)
            ON CONFLICT(sonarr_id) DO NOTHING
            """,
            (sonarr_id, title, now_iso),
        )
        if c.rowcount == 1:
            # We inserted the row → we own the claim.
            return True

        # Row already existed. Inspect it to decide whether it's reclaimable.
        c.execute(
            "SELECT status, processed_at FROM processed_series WHERE sonarr_id = ?",
            (sonarr_id,),
        )
        row = c.fetchone()
        if row is None:
            # Extremely unlikely race (row deleted between INSERT and SELECT);
            # treat as not-claimed so the next cycle retries.
            return False

        status, processed_at = row[0], row[1]
        if status == "done":
            return False

        # status == 'in_progress' (or legacy NULL treated as not-done): reclaim
        # only if the existing claim is stale.
        if _claim_is_stale(processed_at, now):
            c.execute(
                "UPDATE processed_series SET status = 'in_progress', title = ?, processed_at = ? WHERE sonarr_id = ?",
                (title, now_iso, sonarr_id),
            )
            log.warning(
                f"Reclaiming stale 'in_progress' series '{title}' (ID: {sonarr_id}); "
                f"previous claim at {processed_at} exceeded {STALE_CLAIM_MINUTES} min"
            )
            return True
        return False


def _claim_is_stale(processed_at, now):
    """True if an 'in_progress' processed_at timestamp is older than the threshold.

    A missing/unparseable timestamp is treated as stale so a malformed claim can
    never lock a series forever.
    """
    if not processed_at:
        return True
    try:
        claimed = datetime.fromisoformat(processed_at)
    except (ValueError, TypeError):
        return True
    if claimed.tzinfo is None:
        claimed = claimed.replace(tzinfo=timezone.utc)
    return (now - claimed) >= timedelta(minutes=STALE_CLAIM_MINUTES)


def mark_series_done(conn, sonarr_id, title):
    """Flip a claimed series to status='done' after setup completes successfully."""
    with conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO processed_series (sonarr_id, title, status, processed_at) VALUES (?, ?, 'done', ?)",
            (sonarr_id, title, datetime.now(timezone.utc).isoformat()),
        )


def release_series_claim(conn, sonarr_id):
    """Drop an unfinished claim so the series is retried next run.

    Called when setup raises after we won the claim: deleting the row (rather
    than leaving it 'in_progress') lets the very next cycle re-claim immediately
    instead of waiting out STALE_CLAIM_MINUTES.
    """
    try:
        with conn:
            c = conn.cursor()
            c.execute(
                "DELETE FROM processed_series WHERE sonarr_id = ? AND status != 'done'",
                (sonarr_id,),
            )
    except sqlite3.Error as e:
        log.warning(f"Failed to release claim for series {sonarr_id}: {e}")


def is_season_unlocked(conn, sonarr_id, season_number):
    """Check if a season has already been fully unlocked."""
    conn.commit()  # Ensure fresh transaction
    c = conn.cursor()
    c.execute(
        "SELECT unlocked_by, unlocked_at FROM unlocked_seasons WHERE sonarr_id = ? AND season_number = ? ORDER BY unlocked_at DESC LIMIT 1",
        (sonarr_id, season_number)
    )
    row = c.fetchone()
    if row:
        log.debug(f"  Season {season_number} already unlocked by {row[0]} at {row[1]}")
    return row is not None


def mark_season_unlocked(conn, sonarr_id, season_number, unlocked_by):
    """Mark a season as fully unlocked."""
    try:
        with conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO unlocked_seasons (sonarr_id, season_number, unlocked_by, unlocked_at) VALUES (?, ?, ?, ?)",
                (sonarr_id, season_number, unlocked_by, datetime.now(timezone.utc).isoformat())
            )
    except sqlite3.IntegrityError:
        log.debug(f"Season {season_number} already unlocked for series {sonarr_id}")


# ============================================================
# TASK 1: Set monitoring for newly added series
# Requested season = all episodes, Other seasons = Episode 1 only
# ============================================================
def set_initial_monitoring(conn):
    """Find newly added series and set monitoring."""
    log.info("=== Checking for newly added series ===")

    all_series = sonarr_get("/series")
    if not isinstance(all_series, list):
        log.warning("Sonarr /series returned no usable list; skipping new-series check this cycle")
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=NEW_SERIES_LOOKBACK_HOURS)

    newly_added = []
    for series in all_series:
        added_str = series.get("added", "")
        if not added_str:
            continue
        try:
            added_date = datetime.fromisoformat(added_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        if added_date >= cutoff and not is_series_processed(conn, series["id"]):
            newly_added.append(series)

    if not newly_added:
        log.info("No new unprocessed series found")
        return

    log.info(f"Found {len(newly_added)} new series to process")

    for series in newly_added:
        # Isolate each series: a failure (e.g. transient Sonarr error) must not
        # abort the whole batch. process_new_series already releases its claim
        # before re-raising, so the failed series stays re-claimable next cycle.
        try:
            process_new_series(conn, series)
        except Exception as e:
            log.error(f"Failed to process new series '{series.get('title', series.get('id'))}': {e}", exc_info=True)
            continue


def determine_target_season(series, episodes):
    """Determine which season to fully download.

    Priority:
    1. Query Seerr for the actually requested season(s)
    2. If episodes have files, use the lowest season with files
    3. Fallback to Season 1
    """
    title = series["title"]
    tvdb_id = series.get("tvdbId")

    # All seasons excluding specials
    seasons = set()
    seasons_with_files = set()
    for ep in episodes:
        sn = ep.get("seasonNumber", 0)
        if sn == 0:
            continue
        seasons.add(sn)
        if ep.get("hasFile"):
            seasons_with_files.add(sn)

    if not seasons:
        return None, set()

    # 1. Ask Seerr which seasons were requested
    if tvdb_id:
        requested = get_requested_seasons_from_seerr(tvdb_id, title)
        if requested is not None:  # Request exists (could be empty set or populated set)
            if not requested:  # Empty set = "remaining seasons"
                target = min(seasons - seasons_with_files) if seasons - seasons_with_files else min(seasons)
                log.info(f"  Seerr 'remaining seasons' request - defaulting to Season {target}")
                return target, {target}
            elif requested >= seasons or len(requested) >= len(seasons):
                # If ALL (or nearly all) seasons were requested, treat as
                # "Season 1 full + E01 of the rest" to avoid downloading everything
                target = min(seasons)
                log.info(f"  All seasons requested - defaulting to Season {target} full + E01 of rest")
                return target, {target}
            return min(requested), requested

    # 2. If episodes have files, use the lowest season with files
    if seasons_with_files:
        target = min(seasons_with_files)
        log.info(f"  Detected existing files in Season {target}")
        return target, seasons_with_files

    # 3. Fallback to Season 1
    target = min(seasons)
    log.info(f"  No Seerr data or files found, defaulting to Season {target}")
    return target, {target}


def apply_monitoring(series_id, title, episodes, target_seasons, all_seasons):
    """Set episode monitoring: full download for target seasons, E01 only for others.

    Args:
        target_seasons: set of season numbers to fully monitor

    Issues three Sonarr API calls instead of one per episode:
      1. PUT /series/{id}            — flips seasons[].monitored. Goes FIRST so
         Sonarr stops auto-searching unmonitored seasons as quickly as possible.
      2. PUT /episode/monitor (unmonitor list) — bulk-flip everything that
         should be off (E02+ of preview seasons, specials).
      3. PUT /episode/monitor (monitor list)   — bulk-flip preview E01s on.

    The original implementation looped sonarr_put(f"/episode/{id}", ep) for
    every episode, taking ~4 seconds for a typical series. Sonarr's
    auto-search-on-add enumerates monitored episode IDs at T≈0.1s and pushes
    NZBs to SABnzbd faster than that loop could complete, so unwanted seasons
    were grabbed before we could unmonitor them (2026-05-25 Killer Cases).
    The bulk endpoint reduces the race window to ~250ms.
    """
    to_monitor = []
    to_unmonitor = []
    dry_run_changes = []

    for ep in episodes:
        season = ep.get("seasonNumber", 0)
        episode_num = ep.get("episodeNumber", 0)

        if season == 0:
            should_monitor = False
        elif season in target_seasons:
            should_monitor = True
        elif episode_num == 1:
            should_monitor = True
        else:
            should_monitor = False

        if ep.get("monitored") == should_monitor:
            continue

        if should_monitor:
            to_monitor.append(ep["id"])
        else:
            to_unmonitor.append(ep["id"])

        if DRY_RUN:
            action = "MONITOR" if should_monitor else "UNMONITOR"
            dry_run_changes.append(
                f"  [DRY RUN] Would {action}: {title} S{season:02d}E{episode_num:02d}"
            )

    changes_made = len(to_monitor) + len(to_unmonitor)

    # Update season-level monitored flags. Non-target seasons MUST be False —
    # setting them True causes Sonarr to re-monitor every episode within and
    # undo our preview-only setup.
    series_detail = sonarr_get(f"/series/{series_id}")
    if not isinstance(series_detail, dict):
        # Without the series body we cannot flip the season-level monitored gate
        # (which MUST go first). Bail rather than push a None body or apply
        # episode flips without the gate and trigger unwanted Sonarr searches.
        log.warning(f"  Could not fetch series detail for {title} (id {series_id}); skipping monitoring update this pass")
        return 0
    for season_info in series_detail.get("seasons", []):
        sn = season_info["seasonNumber"]
        season_info["monitored"] = sn in target_seasons

    if DRY_RUN:
        for line in dry_run_changes:
            log.info(line)
    else:
        # Order matters: season-level gate first, then unmonitor, then monitor.
        sonarr_put(f"/series/{series_id}", series_detail)
        if to_unmonitor:
            sonarr_put("/episode/monitor", {"episodeIds": to_unmonitor, "monitored": False})
        if to_monitor:
            sonarr_put("/episode/monitor", {"episodeIds": to_monitor, "monitored": True})

    other_count = len(all_seasons - target_seasons)
    log.info(f"  Set monitoring for {title}: Seasons {sorted(target_seasons)} (full) + E01 of {other_count} other seasons ({changes_made} changes)")

    return changes_made


def process_new_series(conn, series):
    """Set monitoring for a single new series based on Seerr request.

    Requires Jellyseerr/Overseerr to have "Enable Automatic Search" UNCHECKED on
    the Sonarr server config. Without that, Sonarr fires MissingEpisodeSearch
    the same second the series is added, snapshots every monitored episode ID
    (Sonarr's default = all monitored), and serially grabs them regardless of
    any subsequent monitor flip. v1.2.2 narrowed the monitor-flip window;
    v1.2.3 tried to DELETE the running search command and discovered Sonarr
    returns 409 Conflict for any command in `started` status (transitions
    queued→started in <1 s). v1.2.4 removed the cancel attempt and moved the
    fix to the Jellyseerr config layer instead. See README "Required external
    configuration" for the setting.
    """
    series_id = series["id"]
    title = series["title"]

    log.info(f"Processing new series: {title} (ID: {series_id})")

    episodes = sonarr_get(f"/episode?seriesId={series_id}")
    if not episodes:
        log.warning(f"No episodes found for {title}")
        return

    # All non-special seasons
    all_seasons = set()
    for ep in episodes:
        if ep.get("seasonNumber", 0) > 0:
            all_seasons.add(ep["seasonNumber"])

    if not all_seasons:
        log.warning(f"No regular seasons found for {title}")
        return

    # Determine which season(s) to fully download
    target_season, target_seasons = determine_target_season(series, episodes)

    if target_season is None:
        log.warning(f"Could not determine target season for {title}")
        return

    # Atomically claim this series before doing any setup. The claim is the
    # cross-process lock the in-memory scheduler locks cannot provide: the poll
    # subprocess and a SeriesAdd webhook subprocess can hit the same newly-added
    # series concurrently. Only the winner runs apply_monitoring + searches; a
    # loser (or a re-entry while still 'in_progress') skips out here, preventing
    # duplicate monitor flips / SeasonSearch / SABnzbd grabs. The row is left
    # 'in_progress' — NOT 'done' — until the setup block below completes, so a
    # crash mid-setup is retried (durability fix for the old "mark done at the
    # start" bug that permanently flagged a show whose monitoring never applied).
    if not claim_series_for_processing(conn, series_id, title):
        log.info(f"  Skipping {title} (ID: {series_id}): already claimed/processed by another run")
        return

    try:
        # Apply monitoring rules IMMEDIATELY to reduce the window in which Sonarr's
        # auto-search can queue unwanted episodes. Sonarr begins searching for all
        # monitored episodes as soon as a series is added — if we wait before setting
        # monitoring, it will have already queued S2/S3 before we can stop it.
        apply_monitoring(series_id, title, episodes, target_seasons, all_seasons)

        # Also wait for Sonarr to finish its initial processing, then re-apply.
        # Sonarr's background tasks (episode import, auto-search) run after SeriesAdd
        # and may re-monitor everything, undoing our changes. Re-applying after the
        # wait ensures we correct any monitoring that Sonarr reset during processing.
        if not DRY_RUN:
            log.info(f"  Waiting 15s for Sonarr to finish initial processing...")
            time.sleep(15)
            # Re-fetch and re-apply in case Sonarr re-monitored during initial processing.
            # If the re-fetch returns no usable list, keep the prior good episodes
            # rather than passing None into apply_monitoring (which would crash).
            refetched = sonarr_get(f"/episode?seriesId={series_id}")
            if isinstance(refetched, list) and refetched:
                episodes = refetched
            else:
                log.warning(f"  Episode re-fetch for {title} returned no usable list; reusing prior snapshot")
            apply_monitoring(series_id, title, episodes, target_seasons, all_seasons)

        # Trigger search ONLY for the target seasons (not the whole series)
        if not DRY_RUN:
            for sn in target_seasons:
                try:
                    sonarr_post("/command", {
                        "name": "SeasonSearch",
                        "seriesId": series_id,
                        "seasonNumber": sn
                    })
                    log.info(f"  Triggered search for {title} Season {sn}")
                except Exception as e:
                    log.warning(f"  Failed to trigger search for {title} S{sn:02d}: {e}")

            # Search for E01 of all non-target preview seasons
            other_seasons = all_seasons - target_seasons
            if other_seasons:
                for sn in sorted(other_seasons):
                    # Find E01 episode ID for this season
                    e01_episodes = [ep for ep in episodes if ep.get("seasonNumber") == sn and ep.get("episodeNumber") == 1]
                    if e01_episodes:
                        try:
                            sonarr_post("/command", {
                                "name": "EpisodeSearch",
                                "episodeIds": [e01_episodes[0]["id"]]
                            })
                            log.info(f"  Triggered search for {title} S{sn:02d}E01 (preview)")
                        except Exception as e:
                            log.warning(f"  Failed to trigger E01 search for {title} S{sn:02d}: {e}")

        # Aggressive queue cleanup + re-apply monitoring in case Sonarr
        # re-monitored episodes during search. All 3 passes always run — no early exit.
        # Skipping passes based on "0 cancelled" is unsafe: 0 could mean downloads
        # already completed (damage done) or not queued yet (pass too early). Always
        # re-apply monitoring on each pass in case Sonarr reset it again.
        if not DRY_RUN:
            for delay in [10, 20, 30]:
                try:
                    time.sleep(delay)
                    # Re-fetch and re-apply monitoring each pass. Reuse the prior
                    # snapshot if a pass returns no usable list (don't pass None on).
                    refetched = sonarr_get(f"/episode?seriesId={series_id}")
                    if isinstance(refetched, list) and refetched:
                        episodes = refetched
                    apply_monitoring(series_id, title, episodes, target_seasons, all_seasons)
                    cleanup_unwanted_queue_items(series_id, title)
                except Exception as e:
                    log.warning(f"  Cleanup pass failed: {e}")

        # Unlock target seasons now that setup has succeeded.
        for sn in target_seasons:
            mark_season_unlocked(conn, series_id, sn, "initial_setup")
    except Exception:
        # Setup failed AFTER we won the claim (e.g. a transient Sonarr error in
        # apply_monitoring). Drop the claim so the series is re-claimable and
        # retried on the next cycle, instead of being locked 'in_progress' with
        # its monitoring never applied (the old durability bug).
        log.error(f"Setup failed for {title} (ID: {series_id}); releasing claim for retry", exc_info=True)
        release_series_claim(conn, series_id)
        raise

    # Flip to 'done' only after the full setup block above completed without
    # raising. This is the single point at which the series is considered
    # permanently configured.
    mark_series_done(conn, series_id, title)


def process_single_series(conn, series_id):
    """Process a single series by Sonarr ID (called from webhook handler)."""
    log.info(f"=== Webhook-triggered processing for series ID {series_id} ===")

    try:
        series = sonarr_get(f"/series/{series_id}")
    except Exception as e:
        log.error(f"Could not fetch series {series_id}: {e}")
        return

    if not series:
        log.error(f"Series {series_id} not found in Sonarr")
        return

    if is_series_processed(conn, series_id):
        log.info(f"Series '{series.get('title', series_id)}' already processed, running queue cleanup")
        # Still do queue cleanup in case there are unwanted items
        cleanup_unwanted_queue_items(series_id, series.get("title", str(series_id)))
        return

    process_new_series(conn, series)


def cleanup_unwanted_queue_items(series_id, title):
    """Cancel queued downloads for unmonitored episodes of a series.
    Returns number of cancelled items."""
    try:
        episodes = sonarr_get(f"/episode?seriesId={series_id}")
    except Exception as e:
        log.warning(f"  Failed to get episodes for queue cleanup: {e}")
        return 0

    if not isinstance(episodes, list):
        log.warning(f"  No usable episode list for queue cleanup of {title}; skipping")
        return 0

    unmonitored_episode_ids = set()
    for ep in episodes:
        if not ep.get("monitored"):
            unmonitored_episode_ids.add(ep["id"])

    if not unmonitored_episode_ids:
        return 0

    cancelled = 0
    page = 1
    page_size = 100

    while True:
        try:
            queue_data = sonarr_get(f"/queue?page={page}&pageSize={page_size}&includeUnknownSeriesItems=false")
        except Exception as e:
            log.warning(f"  Failed to get queue page {page}: {e}")
            break

        if not queue_data:
            break

        records = queue_data.get("records", [])
        if not records:
            break

        for item in records:
            if item.get("seriesId") != series_id:
                continue

            episode_id = item.get("episodeId")
            if episode_id in unmonitored_episode_ids:
                try:
                    sonarr_delete(f"/queue/{item['id']}?removeFromClient=true&blocklist=false")
                    cancelled += 1
                except Exception as e:
                    log.warning(f"  Failed to cancel queue item {item['id']}: {e}")

        total_records = queue_data.get("totalRecords", 0)
        if page * page_size >= total_records:
            break

        page += 1

    if cancelled > 0:
        log.info(f"  Cancelled {cancelled} unwanted downloads for {title}")

    return cancelled


# ============================================================
# PHASE 3: auto-resolve "matched by ID" import blocks
# ============================================================
# When a completed download's release name doesn't self-parse to a library
# title (e.g. "Battlestar.Galactica.2005.S02E01" vs "Battlestar Galactica
# (2003)"), Sonarr/Radarr match it only via grab-history ID and then refuse to
# auto-import as a safety measure. The queue item sits in importBlocked/
# importPending with a status message like "...matched to series by ID.
# Automatic import is not possible. ... Manual Import required." We detect that
# specific signature, pull the resolved candidate via /manualimport, and fire a
# ManualImport command to clear it. We deliberately ignore genuinely-unparseable
# junk ("No files are eligible for import") and benign quality rejections
# ("Not a Custom Format upgrade").

# Tracked-download states that indicate the item is held pending a manual
# import decision (i.e. a real import block, not still downloading).
_IMPORT_BLOCKED_STATES = {"importblocked", "importpending"}


def _is_matched_by_id_block(record):
    """True iff a queue record is a "matched by ID" import block we should auto-clear.

    Conservative on purpose: requires both an import-block tracked state AND a
    status message that carries the distinctive by-ID signature. The two benign
    cases the spec calls out -- season-pack "No files are eligible for import"
    and quality "Not a Custom Format upgrade" rejections -- must NOT match.
    """
    state = str(record.get("trackedDownloadState", "")).strip().lower()
    if state not in _IMPORT_BLOCKED_STATES:
        return False

    # Gather every status message string attached to the record.
    messages = []
    for sm in record.get("statusMessages", []) or []:
        title = sm.get("title")
        if title:
            messages.append(str(title))
        for m in sm.get("messages", []) or []:
            if m:
                messages.append(str(m))
    blob = " ".join(messages).lower()
    if not blob:
        return False

    # by-ID signature: "matched ... by ID" (Sonarr/Radarr phrasing) and/or the
    # "Manual Import required" follow-up. Require the distinctive by-ID phrase so
    # we don't fire on unrelated manual-import nudges.
    matched_by_id = "matched" in blob and "by id" in blob
    if not matched_by_id:
        return False

    # Explicitly steer clear of the benign cases even if some future message
    # ever combined them with by-ID text.
    if "no files are eligible for import" in blob:
        return False
    if "not a custom format upgrade" in blob:
        return False

    return True


def _build_sonarr_import_file(candidate, download_id):
    """Map a Sonarr /manualimport candidate row to a ManualImport `files` entry.

    Carries through the resolved seriesId/episodeIds/quality the manualimport
    response already worked out for us. Returns None if the candidate didn't
    resolve to a series + at least one episode (nothing safe to import).
    """
    series = candidate.get("series") or {}
    series_id = series.get("id") or candidate.get("seriesId")
    episode_ids = [e["id"] for e in (candidate.get("episodes") or []) if e.get("id")]
    if not series_id or not episode_ids:
        return None

    entry = {
        "path": candidate.get("path"),
        "seriesId": series_id,
        "episodeIds": episode_ids,
        "quality": candidate.get("quality"),
        "languages": candidate.get("languages"),
        "releaseGroup": candidate.get("releaseGroup"),
        "downloadId": download_id,
    }
    # Carry through optional fields only when present (mirrors the UI payload).
    if candidate.get("customFormats") is not None:
        entry["customFormats"] = candidate.get("customFormats")
    if candidate.get("indexerFlags") is not None:
        entry["indexerFlags"] = candidate.get("indexerFlags")
    return entry


def _build_radarr_import_file(candidate, download_id):
    """Map a Radarr /manualimport candidate row to a ManualImport `files` entry.

    Radarr uses movieId instead of seriesId/episodeIds. Returns None if the
    candidate didn't resolve to a movie.
    """
    movie = candidate.get("movie") or {}
    movie_id = movie.get("id") or candidate.get("movieId")
    if not movie_id:
        return None

    entry = {
        "path": candidate.get("path"),
        "movieId": movie_id,
        "quality": candidate.get("quality"),
        "languages": candidate.get("languages"),
        "releaseGroup": candidate.get("releaseGroup"),
        "downloadId": download_id,
    }
    if candidate.get("customFormats") is not None:
        entry["customFormats"] = candidate.get("customFormats")
    if candidate.get("indexerFlags") is not None:
        entry["indexerFlags"] = candidate.get("indexerFlags")
    return entry


def _resolve_blocked_imports(label, get_fn, post_fn, build_file_fn):
    """Shared blocked-queue -> manualimport -> ManualImport flow.

    `label` is "Sonarr"/"Radarr" for logs; get_fn/post_fn are the *arr clients;
    build_file_fn maps a manualimport candidate to a ManualImport `files` entry.
    Returns the number of import commands fired (0 in DRY_RUN). Pages the queue
    with the same pattern as cleanup_unwanted_queue_items.
    """
    resolved = 0
    page = 1
    page_size = 100

    while True:
        try:
            queue_data = get_fn(
                f"/queue?page={page}&pageSize={page_size}&includeUnknownSeriesItems=false")
        except Exception as e:
            log.warning(f"  [{label}] Failed to get queue page {page}: {e}")
            break

        if not queue_data:
            break

        records = queue_data.get("records", [])
        if not records:
            break

        for record in records:
            if not _is_matched_by_id_block(record):
                continue

            download_id = record.get("downloadId")
            title = record.get("title", "?")
            if not download_id:
                log.warning(f"  [{label}] by-ID block '{title}' has no downloadId; skipping")
                continue

            if DRY_RUN:
                log.info(f"  [{label}] DRY RUN: would import by-ID block '{title}' (downloadId={download_id})")
                continue

            try:
                candidates = get_fn(f"/manualimport?downloadId={download_id}")
            except Exception as e:
                log.warning(f"  [{label}] manualimport lookup failed for '{title}': {e}")
                continue

            if not isinstance(candidates, list) or not candidates:
                log.warning(f"  [{label}] no manualimport candidates for '{title}'; skipping")
                continue

            files = []
            for cand in candidates:
                entry = build_file_fn(cand, download_id)
                if entry:
                    files.append(entry)

            if not files:
                log.warning(f"  [{label}] '{title}' did not resolve to importable files; skipping")
                continue

            try:
                post_fn("/command", {"name": "ManualImport", "files": files, "importMode": "auto"})
                resolved += 1
                log.info(f"  [{label}] Auto-imported by-ID block '{title}' ({len(files)} file(s))")
            except Exception as e:
                log.warning(f"  [{label}] ManualImport command failed for '{title}': {e}")

        total_records = queue_data.get("totalRecords", 0)
        if page * page_size >= total_records:
            break

        page += 1

    return resolved


def resolve_blocked_imports():
    """Auto-resolve Sonarr "matched by ID" import blocks. Returns count resolved."""
    return _resolve_blocked_imports("Sonarr", sonarr_get, sonarr_post, _build_sonarr_import_file)


def resolve_blocked_imports_radarr():
    """Auto-resolve Radarr "matched by ID" import blocks. Returns count resolved."""
    return _resolve_blocked_imports("Radarr", radarr_get, radarr_post, _build_radarr_import_file)


def resolve_all_blocked_imports():
    """Run both *arr blocked-import resolvers, each gated on its API key and the
    AUTO_IMPORT_BLOCKED master toggle. Each call is wrapped so a failure in one
    can't abort the cycle."""
    if not AUTO_IMPORT_BLOCKED:
        return

    if SONARR_API_KEY:
        try:
            resolve_blocked_imports()
        except Exception as e:
            log.error(f"Sonarr blocked-import resolution failed: {e}", exc_info=True)

    if RADARR_API_KEY:
        try:
            resolve_blocked_imports_radarr()
        except Exception as e:
            log.error(f"Radarr blocked-import resolution failed: {e}", exc_info=True)


def _index_series_by_tvdb(series_by_tvdb, s):
    """Insert a Sonarr series into a tvdbId->series map, keeping the first on collision.

    Two Sonarr entries sharing a tvdbId would otherwise silently overwrite each
    other. We keep the first-seen entry and warn naming both series so the
    duplicate is visible in logs.
    """
    tvdb_id = s.get("tvdbId")
    if not tvdb_id:
        return
    existing = series_by_tvdb.get(tvdb_id)
    if existing is not None:
        log.warning(
            f"Duplicate tvdbId {tvdb_id} in Sonarr: keeping "
            f"'{existing.get('title')}' (id={existing.get('id')}), "
            f"skipping '{s.get('title')}' (id={s.get('id')})"
        )
        return
    series_by_tvdb[tvdb_id] = s


def _resolve_sonarr_series(series_name, series_jf_id, user_id, series_by_title, series_by_tvdb):
    """Match a Jellyfin series to its Sonarr record by title, then TVDB id.

    Shared by the watch-progress cascade and the in-progress boost so both apply
    the same (intentionally conservative) matching: exact case-insensitive title
    first, falling back to a Jellyfin ProviderIds TVDB lookup. Returns the Sonarr
    series dict or None.
    """
    sonarr_series = None
    title_lower = series_name.lower().strip()

    match = series_by_title.get(title_lower)
    if match:
        if isinstance(match, list):
            for candidate in match:
                if candidate["title"].lower().strip() == title_lower:
                    sonarr_series = candidate
                    break
            if not sonarr_series:
                sonarr_series = match[0]
        else:
            if match["title"].lower().strip() == title_lower:
                sonarr_series = match

    if not sonarr_series:
        try:
            jf_series_data = jellyfin_get(f"/Users/{user_id}/Items/{series_jf_id}")
            tvdb_id = jf_series_data.get("ProviderIds", {}).get("Tvdb")
            if tvdb_id:
                sonarr_series = series_by_tvdb.get(int(tvdb_id))
                if sonarr_series:
                    log.debug(f"  Matched '{series_name}' via TVDB ID {tvdb_id}")
        except Exception as e:
            log.debug(f"  Could not get provider IDs for {series_name}: {e}")

    return sonarr_series


def unlock_and_download_season(conn, sonarr_id, season_number, series_name, unlocked_by,
                               sonarr_episodes, force_e02, session=None, stop_event=None):
    """Monitor + search + mark + (interruptible) wait + boost for one season.

    Shared by all three unlock sites (next-season cascade, watched-E01 current
    season, and live-playback E01). `sonarr_episodes` is the already-fetched
    /episode list for the series so callers fetch it once. Returns True if the
    season had episodes and was unlocked, False if there were no episodes for it.

    The ~120s SABnzbd-queue wait is interruptible when stop_event is provided
    (in-process scheduler thread) and a plain blocking sleep otherwise (CLI).
    """
    season_episodes = [e for e in sonarr_episodes if e.get("seasonNumber") == season_number]

    if not season_episodes:
        log.warning(f"  {series_name} Season {season_number} has no episodes in Sonarr")
        return False

    if DRY_RUN:
        log.info(f"  [DRY RUN] Would monitor all {len(season_episodes)} episodes of "
                 f"{series_name} S{season_number:02d}")
    else:
        for ep in season_episodes:
            if not ep.get("monitored"):
                ep["monitored"] = True
                sonarr_put(f"/episode/{ep['id']}", ep, session=session)

        try:
            sonarr_post("/command", {
                "name": "SeasonSearch",
                "seriesId": sonarr_id,
                "seasonNumber": season_number
            }, session=session)
            log.info(f"  Triggered download for {series_name} Season {season_number}")
        except Exception as e:
            log.warning(f"  Failed to trigger search: {e}")

    mark_season_unlocked(conn, sonarr_id, season_number, unlocked_by)

    # Wait for downloads to appear in SABnzbd, then boost. Under the scheduler
    # this wait MUST be interruptible so a SIGTERM doesn't freeze the loop
    # thread / delay shutdown. stop_event=None → plain blocking sleep (CLI).
    if not DRY_RUN:
        log.info(f"  Waiting {SABNZBD_QUEUE_WAIT_SECONDS}s for downloads to appear in SABnzbd...")
        if stop_event is not None:
            if stop_event.wait(SABNZBD_QUEUE_WAIT_SECONDS):
                return True  # shutdown requested — bail out promptly
        else:
            time.sleep(SABNZBD_QUEUE_WAIT_SECONDS)

    boost_season_priority(conn, sonarr_id, season_number, series_name, force_e02=force_e02,
                          session=session, stop_event=stop_event)
    return True


# ============================================================
# TASK 2: Monitor watch progress and unlock next seasons
# ============================================================
def check_watch_progress(conn):
    """Check Jellyfin watch progress and download next season when threshold met."""
    log.info("=== Checking watch progress across all users ===")

    all_series = sonarr_get("/series")
    if not isinstance(all_series, list):
        log.warning("Sonarr /series returned no usable list; skipping watch-progress check this cycle")
        return

    series_by_title = {}
    series_by_tvdb = {}
    for s in all_series:
        title_lower = s["title"].lower().strip()
        if title_lower not in series_by_title:
            series_by_title[title_lower] = s
        else:
            existing = series_by_title[title_lower]
            if isinstance(existing, list):
                existing.append(s)
            else:
                series_by_title[title_lower] = [existing, s]

        _index_series_by_tvdb(series_by_tvdb, s)

    for user_id in JELLYFIN_USER_IDS:
        check_user_progress(conn, user_id, series_by_title, series_by_tvdb, all_series)


def check_user_progress(conn, user_id, series_by_title, series_by_tvdb, all_series):
    """Check a single user's watch progress."""
    try:
        user_info = jellyfin_get(f"/Users/{user_id}")
        user_name = user_info.get("Name", user_id)
    except Exception as e:
        log.warning(f"Could not get user info for {user_id}: {e}")
        user_name = user_id

    log.info(f"Checking progress for user: {user_name}")

    try:
        watched_data = jellyfin_get(f"/Users/{user_id}/Items", params={
            "IncludeItemTypes": "Episode",
            "Recursive": "true",
            "IsPlayed": "true",
            "Fields": "SeriesName,ParentIndexNumber,IndexNumber,ProviderIds,UserData",
            "Limit": "10000"
        })
    except Exception as e:
        log.warning(f"Could not get watched episodes for {user_name}: {e}")
        return

    watched_items = watched_data.get("Items", [])
    if not watched_items:
        log.info(f"  No watched episodes found for {user_name}")
        return

    series_progress = {}
    for item in watched_items:
        series_name = item.get("SeriesName", "")
        series_jf_id = item.get("SeriesId", "")
        season_num = item.get("ParentIndexNumber", 0)

        if not series_name or not series_jf_id or season_num == 0:
            continue

        if series_jf_id not in series_progress:
            series_progress[series_jf_id] = {
                "name": series_name,
                "seasons": {}
            }

        seasons = series_progress[series_jf_id]["seasons"]
        if season_num not in seasons:
            seasons[season_num] = set()
        ep_index = item.get("IndexNumber")
        if ep_index is not None:
            seasons[season_num].add(ep_index)

    for series_jf_id, progress_data in series_progress.items():
        series_name = progress_data["name"]

        sonarr_series = _resolve_sonarr_series(
            series_name, series_jf_id, user_id, series_by_title, series_by_tvdb)

        if not sonarr_series:
            log.debug(f"  '{series_name}' not found in Sonarr")
            continue

        sonarr_id = sonarr_series["id"]

        # Fetch the Sonarr episode list ONCE per series. It drives both the
        # aired-count denominator (Bug 1) and the actual monitor/search of any
        # season we unlock. If it isn't a usable list we can't compute aired
        # counts, so skip all unlocks for this series this cycle rather than
        # fall back to the Jellyfin (downloaded-only) count (the original bug).
        sonarr_episodes = sonarr_get(f"/episode?seriesId={sonarr_id}")
        if not isinstance(sonarr_episodes, list):
            log.warning(f"  No usable Sonarr episode list for {series_name}; "
                        f"skipping unlocks this cycle")
            continue

        now = datetime.now(timezone.utc)

        def _aired_count(season):
            count = 0
            for e in sonarr_episodes:
                if e.get("seasonNumber") != season:
                    continue
                ad = e.get("airDateUtc")
                if not ad:
                    continue
                try:
                    aired = datetime.fromisoformat(ad.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if aired <= now:
                    count += 1
            return count

        for season_num, watched_eps in progress_data["seasons"].items():
            watched_count = len(watched_eps)

            # Bug 2: watching a preview E01 unlocks THAT season (independent of
            # the next-season cascade). With Bug 1's aired denominator, watching
            # only E01 is a low % so the next season won't unlock from it.
            if 1 in watched_eps and not is_season_unlocked(conn, sonarr_id, season_num):
                log.info(f"  {user_name} watched E01 of {series_name} S{season_num:02d} "
                         f"-> Unlocking current Season {season_num}")
                unlock_and_download_season(
                    conn, sonarr_id, season_num, series_name,
                    f"watched-e01:{user_name}", sonarr_episodes, force_e02=True)

            next_season = season_num + 1

            if is_season_unlocked(conn, sonarr_id, next_season):
                continue

            # Bug 1: denominator is AIRED episodes (per Sonarr), not the count of
            # episodes Jellyfin can see (downloaded). Preview-only seasons would
            # otherwise read as 100% watched off a single downloaded E01.
            aired_count = _aired_count(season_num)
            if aired_count == 0:
                continue

            progress = watched_count / aired_count

            if progress < WATCH_THRESHOLD:
                continue

            next_season_episodes = [e for e in sonarr_episodes if e.get("seasonNumber") == next_season]
            if not next_season_episodes:
                log.debug(f"  {series_name} Season {next_season} doesn't exist in Sonarr")
                continue

            log.info(f"  {user_name} watched {watched_count}/{aired_count} of {series_name} "
                     f"S{season_num:02d} ({progress:.0%}) -> Unlocking Season {next_season}")

            unlock_and_download_season(
                conn, sonarr_id, next_season, series_name,
                user_name, sonarr_episodes, force_e02=False)


# ============================================================
# TASK 3: Process existing series that were added before this script
# ============================================================
def process_existing_series(conn):
    """Process all existing series that haven't been set up with our monitoring logic."""
    log.info("=== Processing existing series (catch-up) ===")

    all_series = sonarr_get("/series")
    if not isinstance(all_series, list):
        log.warning("Sonarr /series returned no usable list; skipping catch-up this run")
        return
    unprocessed = [s for s in all_series if not is_series_processed(conn, s["id"])]

    if not unprocessed:
        log.info("All existing series already processed")
        return

    log.info(f"Found {len(unprocessed)} existing series to process")

    for series in unprocessed:
        series_id = series["id"]
        title = series["title"]
        # Isolate each series so one failure (transient Sonarr error, etc.) does
        # not abort the whole catch-up batch and does not leave a show locked.
        try:
            episodes = sonarr_get(f"/episode?seriesId={series_id}")
            if not isinstance(episodes, list):
                log.warning(f"  No usable episode list for {title} (id {series_id}); skipping (will retry next run)")
                continue
            has_files = any(ep.get("hasFile") for ep in episodes)

            if has_files:
                # Atomically claim before mutating monitoring so a concurrent
                # webhook/poll subprocess cannot also re-do this setup. Only the
                # winner proceeds; the row stays 'in_progress' until we finish.
                if not claim_series_for_processing(conn, series_id, title):
                    log.info(f"  Skipping existing series {title} (ID: {series_id}): claimed by another run")
                    continue

                try:
                    seasons_with_files = set()
                    for ep in episodes:
                        if ep.get("hasFile") and ep.get("seasonNumber", 0) > 0:
                            seasons_with_files.add(ep["seasonNumber"])

                    for sn in seasons_with_files:
                        mark_season_unlocked(conn, series_id, sn, "existing_content")

                    all_seasons = set(ep.get("seasonNumber", 0) for ep in episodes if ep.get("seasonNumber", 0) > 0)
                    seasons_without_files = all_seasons - seasons_with_files

                    changes = 0
                    for ep in episodes:
                        season = ep.get("seasonNumber", 0)
                        episode_num = ep.get("episodeNumber", 0)

                        if season == 0:
                            should_monitor = False
                        elif season in seasons_with_files:
                            continue
                        elif episode_num == 1:
                            should_monitor = True
                        else:
                            should_monitor = False

                        if ep.get("monitored") != should_monitor:
                            if not DRY_RUN:
                                ep["monitored"] = should_monitor
                                sonarr_put(f"/episode/{ep['id']}", ep)
                            changes += 1

                    if changes > 0:
                        log.info(f"  Adjusted {title}: {len(seasons_with_files)} seasons with files, "
                                 f"{len(seasons_without_files)} seasons set to E01 only ({changes} changes)")
                except Exception:
                    # Setup raised after we claimed → release so it retries.
                    release_series_claim(conn, series_id)
                    raise

                # Flip to done only after the has-files setup completed cleanly.
                mark_series_done(conn, series_id, title)
            else:
                # process_new_series owns its own claim + 'done' flip (and its
                # own claim release on failure), so do NOT claim or mark done here.
                process_new_series(conn, series)
        except Exception as e:
            log.error(f"Failed to process existing series '{title}' (ID: {series_id}): {e}", exc_info=True)
            continue


# ============================================================
# SABNZBD PRIORITY BOOST
# ============================================================
def is_season_boosted(conn, sonarr_id, season_number):
    """Check if a season has already had its priority boosted."""
    c = conn.cursor()
    c.execute(
        "SELECT 1 FROM priority_boosts WHERE sonarr_id = ? AND season_number = ?",
        (sonarr_id, season_number)
    )
    return c.fetchone() is not None


def mark_season_boosted(conn, sonarr_id, season_number):
    """Record that a season's downloads have been priority boosted."""
    try:
        with conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO priority_boosts (sonarr_id, season_number, boosted_at) VALUES (?, ?, ?)",
                (sonarr_id, season_number, datetime.now(timezone.utc).isoformat())
            )
    except sqlite3.IntegrityError:
        pass


def is_episode_boosted(conn, sonarr_id, season_number, episode_number):
    """Check if a single episode has already had its priority boosted."""
    c = conn.cursor()
    c.execute(
        "SELECT 1 FROM episode_boosts WHERE sonarr_id = ? AND season_number = ? AND episode_number = ?",
        (sonarr_id, season_number, episode_number)
    )
    return c.fetchone() is not None


def mark_episode_boosted(conn, sonarr_id, season_number, episode_number):
    """Record that a single episode's download has been priority boosted."""
    try:
        with conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO episode_boosts "
                "(sonarr_id, season_number, episode_number, boosted_at) VALUES (?, ?, ?, ?)",
                (sonarr_id, season_number, episode_number, datetime.now(timezone.utc).isoformat())
            )
    except sqlite3.IntegrityError:
        pass


def boost_season_priority(conn, series_id, season_number, title, force_e02=False, max_retries=3,
                          session=None, stop_event=None):
    """Boost SABnzbd download priority for a season's queued episodes.

    Args:
        force_e02: If True, set E02 to Force (2) and E03+ to High (1).
                   If False, set all episodes to High (1).
        max_retries: Number of attempts to find downloads in SABnzbd queue (default: 3)
        session: Optional thread-confined requests.Session for the in-process
                 playback loop; threaded into every Sonarr/SABnzbd call.
        stop_event: Optional threading.Event. When set (scheduler shutdown), the
                 retry-backoff waits return immediately so the loop can exit
                 promptly. None preserves the blocking time.sleep behavior for the
                 standalone `playback` CLI.
    """
    if not SABNZBD_API_KEY:
        log.info(f"  SABnzbd not configured, skipping priority boost for {title} S{season_number:02d}")
        return

    if is_season_boosted(conn, series_id, season_number):
        log.debug(f"  {title} S{season_number:02d} already boosted, skipping")
        return

    # Get episode info to map episode IDs to episode numbers
    try:
        episodes = sonarr_get(f"/episode?seriesId={series_id}", session=session)
    except Exception as e:
        log.warning(f"  Failed to get episodes for priority boost: {e}")
        return

    if not isinstance(episodes, list):
        log.warning(f"  No usable episode list for priority boost of {title} S{season_number:02d}; skipping")
        return

    ep_number_map = {}
    for ep in episodes:
        if ep.get("seasonNumber") == season_number:
            ep_number_map[ep["id"]] = ep.get("episodeNumber", 0)

    # Retry loop with exponential backoff
    wait_intervals = [SABNZBD_QUEUE_WAIT_SECONDS, 30, 60]  # Initial wait, then +30s, +60s
    boosted = 0

    for attempt in range(max_retries):
        # Wait before checking queue (except we already waited before calling this function on first attempt)
        if attempt > 0:
            wait_time = wait_intervals[min(attempt, len(wait_intervals) - 1)]
            log.info(f"  Attempt {attempt + 1}/{max_retries}: Waiting {wait_time}s before checking SABnzbd queue...")
            # Interruptible backoff: under the scheduler's in-process loop, a
            # shutdown signal sets stop_event and we bail immediately instead of
            # holding the long-lived scheduler thread in a blocking sleep. The
            # standalone CLI passes stop_event=None → plain time.sleep (unchanged).
            if stop_event is not None:
                if stop_event.wait(wait_time):
                    return
            else:
                time.sleep(wait_time)
        else:
            log.info(f"  Attempt {attempt + 1}/{max_retries}: Checking SABnzbd queue...")

        # Get Sonarr queue to find download IDs for this season's episodes
        try:
            sonarr_queue = sonarr_get(f"/queue?page=1&pageSize=200&includeUnknownSeriesItems=false", session=session)
        except Exception as e:
            log.warning(f"  Failed to get Sonarr queue for priority boost: {e}")
            if attempt == max_retries - 1:
                return
            continue

        if not sonarr_queue:
            if attempt == max_retries - 1:
                return
            continue

        records = sonarr_queue.get("records", [])

        # Find queue items for this series/season and collect their download IDs
        download_ids = {}  # download_id -> episode_number
        for item in records:
            if item.get("seriesId") != series_id:
                continue
            ep_id = item.get("episodeId")
            if ep_id not in ep_number_map:
                continue
            dl_id = item.get("downloadId")
            if dl_id:
                download_ids[dl_id] = ep_number_map[ep_id]

        if not download_ids:
            if attempt == max_retries - 1:
                log.info(f"  No SABnzbd queue items found for {title} S{season_number:02d} after {max_retries} attempts")
                return
            log.debug(f"  No queue items found yet, will retry...")
            continue

        # Get SABnzbd queue and match by nzo_id
        sab_slots = sabnzbd_get_queue(session=session)

        for slot in sab_slots:
            nzo_id = slot.get("nzo_id", "")
            if nzo_id not in download_ids:
                continue

            ep_num = download_ids[nzo_id]

            if force_e02 and ep_num == 2:
                priority = 2  # Force
                priority_name = "Force"
            elif force_e02 and ep_num >= 3:
                priority = 1  # High
                priority_name = "High"
            elif not force_e02:
                priority = 1  # High
                priority_name = "High"
            else:
                continue

            if DRY_RUN:
                log.info(f"  [DRY RUN] Would set {title} S{season_number:02d}E{ep_num:02d} "
                         f"to {priority_name} priority in SABnzbd")
            else:
                if sabnzbd_set_priority(nzo_id, priority, session=session):
                    log.info(f"  Set {title} S{season_number:02d}E{ep_num:02d} "
                             f"to {priority_name} priority in SABnzbd")
                    boosted += 1
                else:
                    log.warning(f"  Failed to set priority for {title} S{season_number:02d}E{ep_num:02d}")

        # If we found and boosted items, we're done
        if boosted > 0 or DRY_RUN:
            mark_season_boosted(conn, series_id, season_number)
            log.info(f"  Priority boost complete for {title} S{season_number:02d}: {boosted} items updated")
            return

        # No items in SABnzbd yet, retry if we have attempts left
        if attempt < max_retries - 1:
            log.debug(f"  Downloads not in SABnzbd yet, will retry...")

    # If we got here, we never found items to boost
    log.info(f"  No items appeared in SABnzbd queue for {title} S{season_number:02d} after {max_retries} attempts")


def _parse_jf_datetime(value):
    """Parse a Jellyfin ISO timestamp into an aware UTC datetime, or None.

    Jellyfin returns timestamps like '2026-06-15T20:11:33.0000000Z'. Python's
    fromisoformat rejects the trailing 'Z' (pre-3.11) and 7-digit fractional
    seconds, so normalize both before parsing.
    """
    if not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Trim over-long fractional seconds (Jellyfin emits 7 digits; Python wants <=6).
    if "." in s:
        head, _, tail = s.partition(".")
        frac = tail
        tz = ""
        for marker in ("+", "-"):
            idx = tail.find(marker)
            if idx != -1:
                frac, tz = tail[:idx], tail[idx:]
                break
        frac = frac[:6]
        s = f"{head}.{frac}{tz}"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def boost_in_progress_episodes(conn):
    """Weekly in-progress boost (Phase 1).

    For each season a user is actively working through — at least one played
    episode, most-recent play within INPROGRESS_BOOST_WINDOW_DAYS — that is
    already unlocked/in-progress (is_season_unlocked True) but has NOT yet
    triggered its next-season unlock, bump that season's still-queued episodes
    to High priority so the next episode lands before the user catches up.

    This is independent of boost_season_priority / the priority_boosts table: it
    uses the per-episode episode_boosts ledger so each queued nzo is boosted (and
    logged) exactly once. No-op when SABnzbd is unconfigured or the window is
    disabled (<= 0). Honors DRY_RUN.
    """
    if not SABNZBD_API_KEY:
        log.debug("In-progress boost: SABnzbd not configured, skipping")
        return

    if INPROGRESS_BOOST_WINDOW_DAYS <= 0:
        log.debug("In-progress boost: INPROGRESS_BOOST_WINDOW_DAYS <= 0, disabled")
        return

    log.info("=== In-progress episode priority boost ===")

    all_series = sonarr_get("/series")
    if not isinstance(all_series, list):
        log.warning("Sonarr /series returned no usable list; skipping in-progress boost this cycle")
        return

    series_by_title = {}
    series_by_tvdb = {}
    for s in all_series:
        title_lower = s["title"].lower().strip()
        if title_lower not in series_by_title:
            series_by_title[title_lower] = s
        else:
            existing = series_by_title[title_lower]
            if isinstance(existing, list):
                existing.append(s)
            else:
                series_by_title[title_lower] = [existing, s]
        _index_series_by_tvdb(series_by_tvdb, s)

    cutoff = datetime.now(timezone.utc) - timedelta(days=INPROGRESS_BOOST_WINDOW_DAYS)

    for user_id in JELLYFIN_USER_IDS:
        _boost_in_progress_for_user(conn, user_id, series_by_title, series_by_tvdb, cutoff)


def _boost_in_progress_for_user(conn, user_id, series_by_title, series_by_tvdb, cutoff):
    """In-progress boost for a single Jellyfin user (see boost_in_progress_episodes)."""
    try:
        watched_data = jellyfin_get(f"/Users/{user_id}/Items", params={
            "IncludeItemTypes": "Episode",
            "Recursive": "true",
            "IsPlayed": "true",
            "Fields": "SeriesName,ParentIndexNumber,IndexNumber,ProviderIds,UserData",
            "Limit": "10000"
        })
    except Exception as e:
        log.warning(f"In-progress boost: could not get watched episodes for {user_id}: {e}")
        return

    watched_items = (watched_data or {}).get("Items", [])
    if not watched_items:
        return

    # Aggregate per (series_jf_id, season): play count + most-recent play time.
    series_progress = {}
    for item in watched_items:
        series_name = item.get("SeriesName", "")
        series_jf_id = item.get("SeriesId", "")
        season_num = item.get("ParentIndexNumber", 0)

        if not series_name or not series_jf_id or season_num == 0:
            continue

        entry = series_progress.setdefault(series_jf_id, {"name": series_name, "last_played": {}})
        last_played = entry["last_played"]

        played_at = _parse_jf_datetime((item.get("UserData") or {}).get("LastPlayedDate"))
        if played_at is not None:
            prev = last_played.get(season_num)
            if prev is None or played_at > prev:
                last_played[season_num] = played_at

    for series_jf_id, progress_data in series_progress.items():
        series_name = progress_data["name"]
        sonarr_series = _resolve_sonarr_series(
            series_name, series_jf_id, user_id, series_by_title, series_by_tvdb)
        if not sonarr_series:
            log.debug(f"  In-progress boost: '{series_name}' not found in Sonarr")
            continue

        sonarr_id = sonarr_series["id"]

        for season_num, played_at in progress_data["last_played"].items():
            # Only seasons whose most-recent play is recent enough.
            if played_at < cutoff:
                continue

            # The current season must be unlocked/in-progress...
            if not is_season_unlocked(conn, sonarr_id, season_num):
                continue

            # ...and must not have already cascaded into the next season (that's
            # the season-unlock path's job, handled by boost_season_priority).
            if is_season_unlocked(conn, sonarr_id, season_num + 1):
                continue

            _boost_in_progress_season(conn, sonarr_id, season_num, series_name)


def _boost_in_progress_season(conn, sonarr_id, season_number, title):
    """Bump still-queued, not-yet-boosted episodes of one in-progress season.

    Mirrors the Sonarr-queue -> SABnzbd nzo matching of boost_season_priority but
    is single-pass (no wait/retry) and dedupes per episode via episode_boosts.
    """
    try:
        episodes = sonarr_get(f"/episode?seriesId={sonarr_id}")
    except Exception as e:
        log.warning(f"  In-progress boost: failed to get episodes for {title}: {e}")
        return

    if not isinstance(episodes, list):
        return

    ep_number_map = {}
    for ep in episodes:
        if ep.get("seasonNumber") == season_number:
            ep_number_map[ep["id"]] = ep.get("episodeNumber", 0)

    if not ep_number_map:
        return

    try:
        sonarr_queue = sonarr_get("/queue?page=1&pageSize=200&includeUnknownSeriesItems=false")
    except Exception as e:
        log.warning(f"  In-progress boost: failed to get Sonarr queue for {title}: {e}")
        return

    if not sonarr_queue:
        return

    # download_id -> episode_number for this series/season's queued items.
    download_ids = {}
    for queue_item in sonarr_queue.get("records", []):
        if queue_item.get("seriesId") != sonarr_id:
            continue
        ep_id = queue_item.get("episodeId")
        if ep_id not in ep_number_map:
            continue
        dl_id = queue_item.get("downloadId")
        if dl_id:
            download_ids[dl_id] = ep_number_map[ep_id]

    if not download_ids:
        return

    sab_slots = sabnzbd_get_queue()
    for slot in sab_slots:
        nzo_id = slot.get("nzo_id", "")
        if nzo_id not in download_ids:
            continue

        ep_num = download_ids[nzo_id]

        if is_episode_boosted(conn, sonarr_id, season_number, ep_num):
            log.debug(f"  {title} S{season_number:02d}E{ep_num:02d} already in-progress boosted, skipping")
            continue

        if DRY_RUN:
            log.info(f"  [DRY RUN] Would set {title} S{season_number:02d}E{ep_num:02d} "
                     f"to High priority (in-progress boost)")
            continue

        if sabnzbd_set_priority(nzo_id, 1):
            log.info(f"  Set {title} S{season_number:02d}E{ep_num:02d} "
                     f"to High priority (in-progress boost)")
            mark_episode_boosted(conn, sonarr_id, season_number, ep_num)
        else:
            log.warning(f"  In-progress boost: failed to set priority for "
                        f"{title} S{season_number:02d}E{ep_num:02d}")


# ============================================================
# PLAYBACK DETECTION (Jellyfin /Sessions polling)
# ============================================================
def check_active_playback(conn, session=None, stop_event=None):
    """Check Jellyfin active sessions for E01 playback on preview-only seasons.
    If detected, unlock the full season and boost download priorities.

    Args:
        session: Optional thread-confined requests.Session. The scheduler's
                 in-process playback loop owns ONE Session (and ONE conn) and
                 passes it here so every Jellyfin/Sonarr/SABnzbd call reuses
                 keep-alive connections on that single thread. None → module-level
                 requests (standalone `playback` CLI, unchanged).
        stop_event: Optional threading.Event (the scheduler's shutdown_event).
                 When set, the long SABnzbd-queue wait — and the retry backoffs in
                 boost_season_priority — return immediately so a SIGTERM doesn't
                 freeze the scheduler or delay shutdown. None → blocking
                 time.sleep (standalone CLI, unchanged).
    """
    log.debug("Checking active playback sessions...")

    try:
        sessions = jellyfin_get("/Sessions", session=session)
    except Exception as e:
        log.warning(f"Failed to get Jellyfin sessions: {e}")
        return

    if not sessions:
        return

    # Pre-fetch Sonarr series data to avoid lazy loading in loop
    try:
        all_series = sonarr_get("/series", session=session)
        if not isinstance(all_series, list):
            log.warning("Sonarr /series returned no usable list; skipping playback check this cycle")
            return
        series_by_title = {}
        series_by_tvdb = {}
        for s in all_series:
            title_lower = s["title"].lower().strip()
            if title_lower not in series_by_title:
                series_by_title[title_lower] = s
            _index_series_by_tvdb(series_by_tvdb, s)
    except Exception as e:
        log.warning(f"Failed to fetch Sonarr series for playback check: {e}")
        return  # Can't proceed without series data

    # NOTE: the loop variable is jf_session (a Jellyfin session dict), distinct
    # from the `session` parameter (the optional HTTP requests.Session) so the
    # two never shadow each other.
    for jf_session in sessions:
        now_playing = jf_session.get("NowPlayingItem")
        if not now_playing:
            continue

        user_id = jf_session.get("UserId", "")
        if user_id not in JELLYFIN_USER_IDS:
            continue

        # Must be an Episode
        if now_playing.get("Type") != "Episode":
            continue

        # Must be E01
        ep_index = now_playing.get("IndexNumber")
        if ep_index != 1:
            log.debug(f"  Playback detected: {jf_session.get('UserName', user_id)} playing {now_playing.get('SeriesName', '')} S{now_playing.get('ParentIndexNumber', 0):02d}E{ep_index:02d} (not E01, skipping)")
            continue

        season_number = now_playing.get("ParentIndexNumber", 0)
        if season_number == 0:
            continue

        series_name = now_playing.get("SeriesName", "")
        series_jf_id = now_playing.get("SeriesId", "")
        user_name = jf_session.get("UserName", user_id)

        log.info(f"  Playback detected: {user_name} playing {series_name} S{season_number:02d}E01")

        # Match to Sonarr series
        sonarr_series = series_by_title.get(series_name.lower().strip())

        if not sonarr_series and series_jf_id:
            try:
                jf_series = jellyfin_get(f"/Items/{series_jf_id}", params={"Fields": "ProviderIds"}, session=session)
                tvdb_id = jf_series.get("ProviderIds", {}).get("Tvdb") if jf_series else None
                if tvdb_id:
                    sonarr_series = series_by_tvdb.get(int(tvdb_id))
            except Exception:
                pass

        if not sonarr_series:
            log.debug(f"  '{series_name}' not found in Sonarr, skipping playback detection")
            continue

        sonarr_id = sonarr_series["id"]

        # Skip if season already unlocked
        conn.commit()  # Ensure fresh read
        if is_season_unlocked(conn, sonarr_id, season_number):
            continue

        log.info(f"  Playback trigger: Unlocking {series_name} Season {season_number} "
                 f"(user {user_name} started E01)")

        # Unlock the full season
        sonarr_episodes = sonarr_get(f"/episode?seriesId={sonarr_id}", session=session)
        if not isinstance(sonarr_episodes, list):
            log.warning(f"  No usable episode list for {series_name}; skipping playback unlock this cycle")
            continue

        unlock_and_download_season(
            conn, sonarr_id, season_number, series_name,
            f"playback:{user_name}", sonarr_episodes, force_e02=True,
            session=session, stop_event=stop_event)

        # Preserve the original "bail out promptly on SIGTERM" semantics: if a
        # shutdown was requested during the helper's wait, stop the loop now.
        if stop_event is not None and stop_event.is_set():
            return


# ============================================================
# DATABASE CLEANUP
# ============================================================
def cleanup_stale_db_entries(conn):
    """Remove DB entries for series that no longer exist in Sonarr.

    This DELETEs every DB row whose series isn't in the Sonarr snapshot, so a
    partial/empty/None response would wipe processed/unlock/boost state and
    trigger mass reprocessing of the whole library. Guard against that: never
    mass-delete from a suspect snapshot — only act on a snapshot that is
    plausibly the full series list.
    """
    try:
        all_series = sonarr_get("/series")

        # Plausibility guard 1: response must be a non-empty list. None (decode
        # error / no usable body) or [] (truncated/empty) is never a valid basis
        # for deletion — an empty Sonarr would mean "the user really has zero
        # series", which is indistinguishable from a failed fetch, so skip.
        if not isinstance(all_series, list) or not all_series:
            log.warning("Skipping stale-DB cleanup: Sonarr /series returned no usable list (refusing to mass-delete from a suspect snapshot)")
            return

        active_ids = {s["id"] for s in all_series}

        c = conn.cursor()
        c.execute("SELECT sonarr_id, title FROM processed_series")
        rows = c.fetchall()

        # Plausibility guard 2: if the DB tracks series but the snapshot overlaps
        # with NONE of them, the snapshot is almost certainly truncated/wrong
        # rather than a legitimate wholesale library change. Bailing here avoids
        # wiping every processed/unlock/boost row at once.
        if rows and not any(sid in active_ids for sid, _ in rows):
            log.warning(
                f"Skipping stale-DB cleanup: none of {len(rows)} tracked series "
                f"appear in the {len(active_ids)}-item Sonarr snapshot (likely truncated)"
            )
            return

        stale_ids = [(sid, title) for sid, title in rows if sid not in active_ids]
        if not stale_ids:
            return

        for sid, title in stale_ids:
            log.info(f"  Cleaning up stale DB entry: '{title}' (ID: {sid})")

        with conn:
            c.execute(
                f"DELETE FROM processed_series WHERE sonarr_id NOT IN ({','.join('?' * len(active_ids))})",
                list(active_ids)
            )
            c.execute(
                f"DELETE FROM unlocked_seasons WHERE sonarr_id NOT IN ({','.join('?' * len(active_ids))})",
                list(active_ids)
            )
            c.execute(
                f"DELETE FROM priority_boosts WHERE sonarr_id NOT IN ({','.join('?' * len(active_ids))})",
                list(active_ids)
            )

        log.info(f"  Removed {len(stale_ids)} stale entries from database")
    except Exception as e:
        log.warning(f"DB cleanup failed: {e}")


# ============================================================
# MAIN
# ============================================================
def run_once():
    """Run all tasks once."""
    log.info("=" * 60)
    log.info("Media Automation starting")
    if DRY_RUN:
        log.info("*** DRY RUN MODE - no changes will be made ***")
    log.info("=" * 60)

    conn = init_db()

    try:
        if not check_db_writable(conn):
            return
        set_initial_monitoring(conn)
        check_watch_progress(conn)
        boost_in_progress_episodes(conn)
        resolve_all_blocked_imports()
        cleanup_stale_db_entries(conn)
    except Exception as e:
        log.error(f"Error during automation run: {e}", exc_info=True)
    finally:
        conn.close()

    log.info("Media Automation run complete")


def run_catchup():
    """One-time catch-up for existing series."""
    log.info("Running one-time catch-up for existing series...")
    conn = init_db()
    try:
        if not check_db_writable(conn):
            return
        process_existing_series(conn)
    except Exception as e:
        log.error(f"Error during catch-up: {e}", exc_info=True)
    finally:
        conn.close()


def run_webhook(series_id):
    """Process a single series triggered by webhook."""
    conn = init_db()
    try:
        if not check_db_writable(conn):
            return
        process_single_series(conn, series_id)
    except Exception as e:
        log.error(f"Error during webhook processing: {e}", exc_info=True)
    finally:
        conn.close()


def run_playback():
    """Check active Jellyfin playback and boost priorities."""
    conn = init_db()
    try:
        if not check_db_writable(conn):
            return
        check_active_playback(conn)
    except Exception as e:
        log.error(f"Error during playback check: {e}", exc_info=True)
    finally:
        conn.close()


def run_reprocess(series_id):
    """Clear a series from the DB and reprocess it."""
    conn = init_db()
    try:
        # Look up the series in Sonarr
        try:
            series = sonarr_get(f"/series/{series_id}")
        except Exception as e:
            log.error(f"Series {series_id} not found in Sonarr: {e}")
            return

        title = series.get("title", str(series_id))
        log.info(f"Reprocessing: {title} (ID: {series_id})")

        # Clear from DB
        with conn:
            c = conn.cursor()
            c.execute("DELETE FROM processed_series WHERE sonarr_id = ?", (series_id,))
            c.execute("DELETE FROM unlocked_seasons WHERE sonarr_id = ?", (series_id,))
        log.info(f"  Cleared DB entries for {title}")

        # Reprocess
        process_new_series(conn, series)

    except Exception as e:
        log.error(f"Error during reprocess: {e}", exc_info=True)
    finally:
        conn.close()


def print_usage():
    """Print CLI usage."""
    print("Usage: media_automation.py [command] [args]")
    print()
    print("Commands:")
    print("  (none)              Run full polling cycle")
    print("  catchup             One-time catch-up for existing series")
    print("  webhook <id>        Process a single series by Sonarr ID (webhook mode)")
    print("  reprocess <id>      Clear DB and reprocess a series by Sonarr ID")
    print("  playback            Check active playback and boost priorities")
    print("  list                List all processed series in the DB")


def run_list():
    """List all processed series in the DB."""
    conn = init_db()
    try:
        c = conn.cursor()
        c.execute("SELECT sonarr_id, title, processed_at FROM processed_series ORDER BY processed_at DESC")
        rows = c.fetchall()
        if not rows:
            print("No processed series in database")
            return
        print(f"{'ID':>5}  {'Title':<40}  {'Processed At'}")
        print("-" * 75)
        for sid, title, processed_at in rows:
            print(f"{sid:>5}  {title:<40}  {processed_at}")

        c.execute("SELECT COUNT(*) FROM unlocked_seasons")
        unlock_count = c.fetchone()[0]
        print(f"\nTotal: {len(rows)} series, {unlock_count} unlocked seasons")
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "catchup":
            run_catchup()
        elif sys.argv[1] == "webhook" and len(sys.argv) > 2:
            try:
                run_webhook(int(sys.argv[2]))
            except ValueError:
                log.error(f"Invalid series ID: {sys.argv[2]}")
                sys.exit(1)
        elif sys.argv[1] == "reprocess" and len(sys.argv) > 2:
            try:
                run_reprocess(int(sys.argv[2]))
            except ValueError:
                log.error(f"Invalid series ID: {sys.argv[2]}")
                sys.exit(1)
        elif sys.argv[1] == "playback":
            run_playback()
        elif sys.argv[1] == "list":
            run_list()
        elif sys.argv[1] in ("help", "--help", "-h"):
            print_usage()
        else:
            run_once()
    else:
        run_once()
