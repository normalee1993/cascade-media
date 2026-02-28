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

# Jellyfin user IDs to monitor (exclude SuggestArr bot)
JELLYFIN_USER_IDS = os.getenv("JELLYFIN_USER_IDS", "").split(",")
JELLYFIN_USER_IDS = [uid.strip() for uid in JELLYFIN_USER_IDS if uid.strip()]

SABNZBD_URL = os.getenv("SABNZBD_URL", "")
SABNZBD_API_KEY = os.getenv("SABNZBD_API_KEY", "")
SABNZBD_QUEUE_WAIT_SECONDS = get_int_env("SABNZBD_QUEUE_WAIT_SECONDS", 120)

# Database path for tracking what we've already processed
DB_PATH = os.getenv("DB_PATH", "/data/media_automation.db")

# Dry run mode
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ============================================================
# API HELPERS
# ============================================================
SONARR_HEADERS = {"X-Api-Key": SONARR_API_KEY, "Content-Type": "application/json"}
JELLYFIN_HEADERS = {"X-Emby-Token": JELLYFIN_API_KEY, "Content-Type": "application/json"}
SEERR_HEADERS = {"X-Api-Key": SEERR_API_KEY, "Content-Type": "application/json"}

def _api_request_with_retry(method, url, headers, max_retries=3, **kwargs):
    """Make API request with retry logic for transient failures."""
    for attempt in range(max_retries):
        try:
            resp = method(url, headers=headers, timeout=30, **kwargs)

            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
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


def sonarr_get(endpoint):
    """GET request to Sonarr API."""
    return _api_request_with_retry(requests.get, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS)


def sonarr_put(endpoint, data):
    """PUT request to Sonarr API."""
    return _api_request_with_retry(requests.put, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS, json=data)


def sonarr_post(endpoint, data):
    """POST request to Sonarr API."""
    return _api_request_with_retry(requests.post, f"{SONARR_URL}/api/v3{endpoint}", SONARR_HEADERS, json=data)


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


def jellyfin_get(endpoint, params=None):
    """GET request to Jellyfin API."""
    return _api_request_with_retry(requests.get, f"{JELLYFIN_URL}{endpoint}", JELLYFIN_HEADERS, params=params)


def seerr_get(endpoint, params=None):
    """GET request to Seerr API."""
    return _api_request_with_retry(requests.get, f"{SEERR_URL}/api/v1{endpoint}", SEERR_HEADERS, params=params)


# ============================================================
# SABNZBD API HELPERS
# ============================================================
def sabnzbd_api(mode, params=None):
    """Generic SABnzbd API call."""
    if not SABNZBD_API_KEY:
        log.warning("SABNZBD_API_KEY not set, skipping SABnzbd call")
        return None

    url = f"{SABNZBD_URL}/api"
    req_params = {"apikey": SABNZBD_API_KEY, "mode": mode, "output": "json"}
    if params:
        req_params.update(params)

    try:
        resp = requests.get(url, params=req_params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error(f"SABnzbd API error (mode={mode}): {e}")
        return None


def sabnzbd_get_queue():
    """Get current SABnzbd queue slots."""
    data = sabnzbd_api("queue")
    if data and "queue" in data:
        return data["queue"].get("slots", [])
    return []


def sabnzbd_set_priority(nzo_id, priority):
    """Set priority for a SABnzbd queue item.
    Priority codes: -1=low, 0=normal, 1=high, 2=force
    """
    result = sabnzbd_api("queue", {"name": "priority", "value": nzo_id, "value2": str(priority)})
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
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_series (
                    sonarr_id INTEGER PRIMARY KEY,
                    title TEXT,
                    processed_at TEXT
                )
            """)
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


def is_series_processed(conn, sonarr_id):
    """Check if we've already set monitoring for this series."""
    c = conn.cursor()
    c.execute("SELECT 1 FROM processed_series WHERE sonarr_id = ?", (sonarr_id,))
    return c.fetchone() is not None


def mark_series_processed(conn, sonarr_id, title):
    """Mark a series as processed."""
    with conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO processed_series (sonarr_id, title, processed_at) VALUES (?, ?, ?)",
            (sonarr_id, title, datetime.now(timezone.utc).isoformat())
        )


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
        process_new_series(conn, series)


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
    """
    changes_made = 0

    for ep in episodes:
        season = ep.get("seasonNumber", 0)
        episode_num = ep.get("episodeNumber", 0)

        if season == 0:
            should_monitor = False
        elif season in target_seasons:
            # Monitor ALL episodes in requested season(s)
            should_monitor = True
        elif episode_num == 1:
            # Monitor ONLY Episode 1 of all other seasons (preview)
            should_monitor = True
        else:
            should_monitor = False

        if ep.get("monitored") != should_monitor:
            if DRY_RUN:
                action = "MONITOR" if should_monitor else "UNMONITOR"
                log.info(f"  [DRY RUN] Would {action}: {title} S{season:02d}E{episode_num:02d}")
            else:
                ep["monitored"] = should_monitor
                sonarr_put(f"/episode/{ep['id']}", ep)
            changes_made += 1

    # Update season-level monitoring in the series object.
    # IMPORTANT: Only target seasons should be monitored at the series level.
    # Setting non-target seasons to monitored=True causes Sonarr to
    # re-monitor all their episodes, undoing our individual changes.
    series_detail = sonarr_get(f"/series/{series_id}")
    for season_info in series_detail.get("seasons", []):
        sn = season_info["seasonNumber"]
        if sn in target_seasons:
            season_info["monitored"] = True
        else:
            # Unmonitor at season level - individual E01 episodes
            # remain monitored and will still be searched/downloaded
            season_info["monitored"] = False

    if not DRY_RUN:
        sonarr_put(f"/series/{series_id}", series_detail)

    other_count = len(all_seasons - target_seasons)
    log.info(f"  Set monitoring for {title}: Seasons {sorted(target_seasons)} (full) + E01 of {other_count} other seasons ({changes_made} changes)")

    return changes_made


def process_new_series(conn, series):
    """Set monitoring for a single new series based on Seerr request."""
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

    # Mark as processed immediately — before any waits or searches — so that
    # the polling loop cannot race with the webhook subprocess and trigger a
    # second identical Season search while this function is still running.
    # (mark_season_unlocked is still called below, after searches complete.)
    mark_series_processed(conn, series_id, title)

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
        # Re-fetch and re-apply in case Sonarr re-monitored during initial processing
        episodes = sonarr_get(f"/episode?seriesId={series_id}")
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
                # Re-fetch and re-apply monitoring each pass
                episodes = sonarr_get(f"/episode?seriesId={series_id}")
                apply_monitoring(series_id, title, episodes, target_seasons, all_seasons)
                cleanup_unwanted_queue_items(series_id, title)
            except Exception as e:
                log.warning(f"  Cleanup pass failed: {e}")

    # Unlock target seasons (series was already marked processed above)
    for sn in target_seasons:
        mark_season_unlocked(conn, series_id, sn, "initial_setup")


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
# TASK 2: Monitor watch progress and unlock next seasons
# ============================================================
def check_watch_progress(conn):
    """Check Jellyfin watch progress and download next season when threshold met."""
    log.info("=== Checking watch progress across all users ===")

    all_series = sonarr_get("/series")

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

        if s.get("tvdbId"):
            series_by_tvdb[s["tvdbId"]] = s

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
            "Fields": "SeriesName,ParentIndexNumber,IndexNumber,ProviderIds",
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
        seasons[season_num] = seasons.get(season_num, 0) + 1

    for series_jf_id, progress_data in series_progress.items():
        series_name = progress_data["name"]

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

        if not sonarr_series:
            log.debug(f"  '{series_name}' not found in Sonarr")
            continue

        sonarr_id = sonarr_series["id"]

        for season_num, watched_count in progress_data["seasons"].items():
            next_season = season_num + 1

            if is_season_unlocked(conn, sonarr_id, next_season):
                continue

            try:
                season_data = jellyfin_get(f"/Shows/{series_jf_id}/Episodes", params={
                    "UserId": user_id,
                    "SeasonNumber": season_num
                })
                season_items = [
                    ep for ep in season_data.get("Items", [])
                    if ep.get("ParentIndexNumber") == season_num
                ]
                total_episodes = len(season_items)
            except Exception as e:
                log.debug(f"  Could not get season info for {series_name} S{season_num:02d}: {e}")
                continue

            if total_episodes == 0:
                continue

            progress = watched_count / total_episodes

            if progress < WATCH_THRESHOLD:
                continue

            sonarr_episodes = sonarr_get(f"/episode?seriesId={sonarr_id}")
            next_season_episodes = [e for e in sonarr_episodes if e.get("seasonNumber") == next_season]

            if not next_season_episodes:
                log.debug(f"  {series_name} Season {next_season} doesn't exist in Sonarr")
                continue

            log.info(f"  {user_name} watched {watched_count}/{total_episodes} of {series_name} "
                     f"S{season_num:02d} ({progress:.0%}) -> Unlocking Season {next_season}")

            if DRY_RUN:
                log.info(f"  [DRY RUN] Would monitor all {len(next_season_episodes)} episodes of "
                         f"{series_name} S{next_season:02d}")
            else:
                for ep in next_season_episodes:
                    if not ep.get("monitored"):
                        ep["monitored"] = True
                        sonarr_put(f"/episode/{ep['id']}", ep)

                try:
                    sonarr_post("/command", {
                        "name": "SeasonSearch",
                        "seriesId": sonarr_id,
                        "seasonNumber": next_season
                    })
                    log.info(f"  Triggered download for {series_name} Season {next_season}")
                except Exception as e:
                    log.warning(f"  Failed to trigger search: {e}")

            mark_season_unlocked(conn, sonarr_id, next_season, user_name)

            # Boost priority in SABnzbd for the newly unlocked season
            if not DRY_RUN:
                log.info(f"  Waiting {SABNZBD_QUEUE_WAIT_SECONDS}s for downloads to appear in SABnzbd...")
                time.sleep(SABNZBD_QUEUE_WAIT_SECONDS)
            boost_season_priority(conn, sonarr_id, next_season, series_name, force_e02=False)


# ============================================================
# TASK 3: Process existing series that were added before this script
# ============================================================
def process_existing_series(conn):
    """Process all existing series that haven't been set up with our monitoring logic."""
    log.info("=== Processing existing series (catch-up) ===")

    all_series = sonarr_get("/series")
    unprocessed = [s for s in all_series if not is_series_processed(conn, s["id"])]

    if not unprocessed:
        log.info("All existing series already processed")
        return

    log.info(f"Found {len(unprocessed)} existing series to process")

    for series in unprocessed:
        episodes = sonarr_get(f"/episode?seriesId={series['id']}")
        has_files = any(ep.get("hasFile") for ep in episodes)

        if has_files:
            seasons_with_files = set()
            for ep in episodes:
                if ep.get("hasFile") and ep.get("seasonNumber", 0) > 0:
                    seasons_with_files.add(ep["seasonNumber"])

            for sn in seasons_with_files:
                mark_season_unlocked(conn, series["id"], sn, "existing_content")

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
                _title = series['title']
                log.info(f"  Adjusted {_title}: {len(seasons_with_files)} seasons with files, "
                         f"{len(seasons_without_files)} seasons set to E01 only ({changes} changes)")
        else:
            process_new_series(conn, series)

        mark_series_processed(conn, series["id"], series["title"])


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


def boost_season_priority(conn, series_id, season_number, title, force_e02=False, max_retries=3):
    """Boost SABnzbd download priority for a season's queued episodes.

    Args:
        force_e02: If True, set E02 to Force (2) and E03+ to High (1).
                   If False, set all episodes to High (1).
        max_retries: Number of attempts to find downloads in SABnzbd queue (default: 3)
    """
    if not SABNZBD_API_KEY:
        log.info(f"  SABnzbd not configured, skipping priority boost for {title} S{season_number:02d}")
        return

    if is_season_boosted(conn, series_id, season_number):
        log.debug(f"  {title} S{season_number:02d} already boosted, skipping")
        return

    # Get episode info to map episode IDs to episode numbers
    try:
        episodes = sonarr_get(f"/episode?seriesId={series_id}")
    except Exception as e:
        log.warning(f"  Failed to get episodes for priority boost: {e}")
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
            time.sleep(wait_time)
        else:
            log.info(f"  Attempt {attempt + 1}/{max_retries}: Checking SABnzbd queue...")

        # Get Sonarr queue to find download IDs for this season's episodes
        try:
            sonarr_queue = sonarr_get(f"/queue?page=1&pageSize=200&includeUnknownSeriesItems=false")
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
        sab_slots = sabnzbd_get_queue()

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
                if sabnzbd_set_priority(nzo_id, priority):
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


# ============================================================
# PLAYBACK DETECTION (Jellyfin /Sessions polling)
# ============================================================
def check_active_playback(conn):
    """Check Jellyfin active sessions for E01 playback on preview-only seasons.
    If detected, unlock the full season and boost download priorities."""
    log.debug("Checking active playback sessions...")

    try:
        sessions = jellyfin_get("/Sessions")
    except Exception as e:
        log.warning(f"Failed to get Jellyfin sessions: {e}")
        return

    if not sessions:
        return

    # Pre-fetch Sonarr series data to avoid lazy loading in loop
    try:
        all_series = sonarr_get("/series")
        series_by_title = {}
        series_by_tvdb = {}
        for s in all_series:
            title_lower = s["title"].lower().strip()
            if title_lower not in series_by_title:
                series_by_title[title_lower] = s
            if s.get("tvdbId"):
                series_by_tvdb[s["tvdbId"]] = s
    except Exception as e:
        log.warning(f"Failed to fetch Sonarr series for playback check: {e}")
        return  # Can't proceed without series data

    for session in sessions:
        now_playing = session.get("NowPlayingItem")
        if not now_playing:
            continue

        user_id = session.get("UserId", "")
        if user_id not in JELLYFIN_USER_IDS:
            continue

        # Must be an Episode
        if now_playing.get("Type") != "Episode":
            continue

        # Must be E01
        ep_index = now_playing.get("IndexNumber")
        if ep_index != 1:
            log.debug(f"  Playback detected: {session.get('UserName', user_id)} playing {now_playing.get('SeriesName', '')} S{now_playing.get('ParentIndexNumber', 0):02d}E{ep_index:02d} (not E01, skipping)")
            continue

        season_number = now_playing.get("ParentIndexNumber", 0)
        if season_number == 0:
            continue

        series_name = now_playing.get("SeriesName", "")
        series_jf_id = now_playing.get("SeriesId", "")
        user_name = session.get("UserName", user_id)

        log.info(f"  Playback detected: {user_name} playing {series_name} S{season_number:02d}E01")

        # Match to Sonarr series
        sonarr_series = series_by_title.get(series_name.lower().strip())

        if not sonarr_series and series_jf_id:
            try:
                jf_series = jellyfin_get(f"/Items/{series_jf_id}", params={"Fields": "ProviderIds"})
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
        sonarr_episodes = sonarr_get(f"/episode?seriesId={sonarr_id}")
        season_episodes = [e for e in sonarr_episodes if e.get("seasonNumber") == season_number]

        if not season_episodes:
            log.warning(f"  {series_name} Season {season_number} has no episodes in Sonarr")
            continue

        if not DRY_RUN:
            for ep in season_episodes:
                if not ep.get("monitored"):
                    ep["monitored"] = True
                    sonarr_put(f"/episode/{ep['id']}", ep)

            try:
                sonarr_post("/command", {
                    "name": "SeasonSearch",
                    "seriesId": sonarr_id,
                    "seasonNumber": season_number
                })
                log.info(f"  Triggered search for {series_name} Season {season_number}")
            except Exception as e:
                log.warning(f"  Failed to trigger search: {e}")

        mark_season_unlocked(conn, sonarr_id, season_number, f"playback:{user_name}")

        # Wait for downloads to appear in SABnzbd, then boost
        if not DRY_RUN:
            log.info(f"  Waiting {SABNZBD_QUEUE_WAIT_SECONDS}s for downloads to appear in SABnzbd...")
            time.sleep(SABNZBD_QUEUE_WAIT_SECONDS)

        boost_season_priority(conn, sonarr_id, season_number, series_name, force_e02=True)


# ============================================================
# DATABASE CLEANUP
# ============================================================
def cleanup_stale_db_entries(conn):
    """Remove DB entries for series that no longer exist in Sonarr."""
    try:
        all_series = sonarr_get("/series")
        active_ids = {s["id"] for s in all_series}

        c = conn.cursor()
        c.execute("SELECT sonarr_id, title FROM processed_series")
        rows = c.fetchall()

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
        set_initial_monitoring(conn)
        check_watch_progress(conn)
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
        process_existing_series(conn)
    except Exception as e:
        log.error(f"Error during catch-up: {e}", exc_info=True)
    finally:
        conn.close()


def run_webhook(series_id):
    """Process a single series triggered by webhook."""
    conn = init_db()
    try:
        process_single_series(conn, series_id)
    except Exception as e:
        log.error(f"Error during webhook processing: {e}", exc_info=True)
    finally:
        conn.close()


def run_playback():
    """Check active Jellyfin playback and boost priorities."""
    conn = init_db()
    try:
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
