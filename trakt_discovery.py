#!/usr/bin/env python3
"""
Trakt Content Discovery for cascade-tv
- Discovers trending, popular, anticipated, and recommended content via Trakt
- Requests discovered content through Seerr (TV → Sonarr, Movies → Radarr)
- TV shows request Season 1 only — cascade-tv's E01-preview logic handles the rest
- OAuth device code flow for Trakt authentication
"""

import requests
import json
import time
import os
import sys
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO"), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S"
)
log = logging.getLogger("trakt-discovery")

# ============================================================
# CONFIGURATION
# ============================================================
def get_float_env(key, default):
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        log.error(f"Invalid {key}='{os.getenv(key)}', must be a number. Using default: {default}")
        return default

def get_int_env(key, default):
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        log.error(f"Invalid {key}='{os.getenv(key)}', must be an integer. Using default: {default}")
        return default

def parse_env_list(key, default=""):
    """Parse a comma-separated env var, stripping inline shell-style comments."""
    raw = os.getenv(key, default)
    comment_pos = raw.find('#')
    if comment_pos >= 0:
        raw = raw[:comment_pos]
    return [item.strip() for item in raw.split(",") if item.strip()]

# Trakt API
TRAKT_CLIENT_ID = os.getenv("TRAKT_CLIENT_ID", "")
TRAKT_CLIENT_SECRET = os.getenv("TRAKT_CLIENT_SECRET", "")
TRAKT_BASE_URL = "https://api.trakt.tv"

# Discovery settings
TRAKT_DISCOVERY_ENABLED = os.getenv("TRAKT_DISCOVERY_ENABLED", "false").lower() == "true"
TRAKT_DISCOVERY_INTERVAL_HOURS = get_int_env("TRAKT_DISCOVERY_INTERVAL_HOURS", 6)
TRAKT_DISCOVER_SHOWS = os.getenv("TRAKT_DISCOVER_SHOWS", "true").lower() == "true"
TRAKT_DISCOVER_MOVIES = os.getenv("TRAKT_DISCOVER_MOVIES", "true").lower() == "true"
TRAKT_LISTS = parse_env_list("TRAKT_LISTS", "recommended,watchlist,trending,popular,anticipated")
TRAKT_MIN_RATING = get_float_env("TRAKT_MIN_RATING", 7.0)
TRAKT_MIN_VOTES = get_int_env("TRAKT_MIN_VOTES", 100)
TRAKT_YEARS = os.getenv("TRAKT_YEARS", "").strip()
TRAKT_GENRES = os.getenv("TRAKT_GENRES", "").strip()
TRAKT_LANGUAGES = os.getenv("TRAKT_LANGUAGES", "en").strip()
TRAKT_MAX_REQUESTS_PER_CYCLE = get_int_env("TRAKT_MAX_REQUESTS_PER_CYCLE", 10)
TRAKT_MAX_SHOW_REQUESTS = get_int_env("TRAKT_MAX_SHOW_REQUESTS", 0)  # 0 = use TRAKT_MAX_REQUESTS_PER_CYCLE
TRAKT_MAX_MOVIE_REQUESTS = get_int_env("TRAKT_MAX_MOVIE_REQUESTS", 0)  # 0 = use TRAKT_MAX_REQUESTS_PER_CYCLE
TRAKT_ITEMS_PER_LIST = get_int_env("TRAKT_ITEMS_PER_LIST", 20)

# Parse year range for application-level filtering (backup for API-level filter)
TRAKT_YEAR_MIN = None
TRAKT_YEAR_MAX = None
if TRAKT_YEARS:
    _year_parts = TRAKT_YEARS.split("-")
    try:
        TRAKT_YEAR_MIN = int(_year_parts[0])
        if len(_year_parts) > 1:
            TRAKT_YEAR_MAX = int(_year_parts[1])
    except ValueError:
        log.error(f"Invalid TRAKT_YEARS='{TRAKT_YEARS}', expected format: 2020-2026")

# Genre exclusion (separate from TRAKT_GENRES inclusion filter)
TRAKT_EXCLUDE_GENRES = [g.lower() for g in parse_env_list("TRAKT_EXCLUDE_GENRES")]

# Content rating filters — use Trakt certification field, NO TMDB key required
# TV: TV-Y, TV-Y7, TV-G, TV-PG, TV-14, TV-MA  |  Movie: G, PG, PG-13, R, NC-17
FILTER_CONTENT_RATINGS = parse_env_list("FILTER_CONTENT_RATINGS")
FILTER_EXCLUDE_CONTENT_RATINGS = parse_env_list("FILTER_EXCLUDE_CONTENT_RATINGS")

# TMDB (optional — enables episode count, show status, show type, network, season filters)
# ⚠ BREAKING CHANGE (v8): TRAKT_MAX_EPISODES → TMDB_MAX_EPISODES
#                          TRAKT_ALLOWED_SHOW_STATUS → TMDB_ALLOWED_SHOW_STATUS
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_MAX_EPISODES = get_int_env("TMDB_MAX_EPISODES", 0)  # 0 = disabled
TMDB_ALLOWED_SHOW_STATUS = parse_env_list("TMDB_ALLOWED_SHOW_STATUS")
TMDB_EXCLUDE_SHOW_TYPES = parse_env_list("TMDB_EXCLUDE_SHOW_TYPES")
TMDB_ALLOWED_NETWORKS = parse_env_list("TMDB_ALLOWED_NETWORKS")
TMDB_MAX_SEASONS = get_int_env("TMDB_MAX_SEASONS", 0)  # 0 = disabled
TMDB_ORIGINAL_LANGUAGE = parse_env_list("TMDB_ORIGINAL_LANGUAGE")  # e.g. ["en"] or ["en","ko"]

# Seerr recheck — days before a skipped_exists record expires so auto-deleted content
# (e.g. Jellysweep purges) can be re-discovered and re-requested. 0 = permanent.
TRAKT_SEERR_RECHECK_DAYS = get_int_env("TRAKT_SEERR_RECHECK_DAYS", 365)

# Premium content bypass (override year + show status filters for high-rated content)
TRAKT_PREMIUM_BYPASS_ENABLED = os.getenv("TRAKT_PREMIUM_BYPASS_ENABLED", "true").lower() == "true"
TRAKT_PREMIUM_BYPASS_MIN_RATING = get_float_env("TRAKT_PREMIUM_BYPASS_MIN_RATING", 8.0)
TRAKT_PREMIUM_BYPASS_LISTS = parse_env_list("TRAKT_PREMIUM_BYPASS_LISTS", "recommended,watchlist")
TRAKT_PREMIUM_BYPASS_FILTERS = parse_env_list("TRAKT_PREMIUM_BYPASS_FILTERS", "year,status")

# Seerr
SEERR_URL = os.getenv("SEERR_URL", "")
SEERR_API_KEY = os.getenv("SEERR_API_KEY", "")
SEERR_HEADERS = {"X-Api-Key": SEERR_API_KEY, "Content-Type": "application/json"}
SEERR_USER_ID = get_int_env("SEERR_USER_ID", 0)  # Seerr user ID to attribute requests to

# Database
DB_PATH = os.getenv("DB_PATH", "/data/media_automation.db")

# Dry run
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Alert webhook — fires only on token refresh failure requiring manual re-auth
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# Token cache — avoids redundant refresh checks within a single discovery cycle
_token_cache = {}  # Keys: 'token', 'cached_at'
TOKEN_CACHE_TTL_SECONDS = 300

# Permanently terminal actions — items with these actions are never re-evaluated.
# skipped_exists is NOT listed here — it is handled separately with a time-based
# expiry (TRAKT_SEERR_RECHECK_DAYS) so that auto-deleted content can be re-discovered.
# All filter-based skips (skipped_year, skipped_genre, etc.) are intentionally absent —
# they are re-evaluated each run so config changes take effect without a DB reset.
TERMINAL_ACTIONS = frozenset({
    "requested",        # Actually sent to Seerr — done
    "skipped_watched",  # Trakt watch history is permanent
    "skipped_no_tmdb",  # No TMDB ID — won't change
})

# ============================================================
# HELPERS
# ============================================================
def qualifies_for_premium_bypass(source, rating, filter_type):
    """Returns True if this item should bypass a specific filter due to high rating.
    filter_type: 'year' or 'status'
    """
    if not TRAKT_PREMIUM_BYPASS_ENABLED:
        return False
    if filter_type not in TRAKT_PREMIUM_BYPASS_FILTERS:
        return False
    if source not in TRAKT_PREMIUM_BYPASS_LISTS:
        return False
    if not rating or rating < TRAKT_PREMIUM_BYPASS_MIN_RATING:
        return False
    return True


# ============================================================
# API HELPERS
# ============================================================
def _api_request_with_retry(method, url, headers, max_retries=3, **kwargs):
    """Make API request with retry logic for transient failures."""
    for attempt in range(max_retries):
        try:
            resp = method(url, headers=headers, timeout=30, **kwargs)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                log.warning(f"Rate limited, waiting {retry_after}s before retry")
                time.sleep(retry_after)
                continue

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


def trakt_get(endpoint, params=None, auth_required=False, conn=None):
    """GET request to Trakt API."""
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID,
    }
    if auth_required and conn:
        token = get_valid_token(conn)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            log.warning("Auth required but no valid token available")
            return None
    return _api_request_with_retry(requests.get, f"{TRAKT_BASE_URL}{endpoint}", headers, params=params)


def trakt_post(endpoint, data=None, headers_override=None):
    """POST request to Trakt API."""
    headers = headers_override or {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_CLIENT_ID,
    }
    return _api_request_with_retry(requests.post, f"{TRAKT_BASE_URL}{endpoint}", headers, json=data)


def _send_alert_webhook(message):
    """Fire optional webhook alert on token failure. Supports Discord (content) and Slack (text)."""
    if not ALERT_WEBHOOK_URL:
        return
    payload = {"content": message, "text": message}
    try:
        requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=10)
        log.info("Alert webhook sent")
    except Exception as e:
        log.warning(f"Alert webhook failed: {e}")


def seerr_get(endpoint):
    """GET request to Seerr API."""
    return _api_request_with_retry(requests.get, f"{SEERR_URL}/api/v1{endpoint}", SEERR_HEADERS)


def seerr_post(endpoint, data):
    """POST request to Seerr API."""
    return _api_request_with_retry(requests.post, f"{SEERR_URL}/api/v1{endpoint}", SEERR_HEADERS, json=data)


# ============================================================
# TMDB HELPERS (optional — episode count, status, rating filters)
# ============================================================
_tmdb_cache = {}


def tmdb_get(endpoint, params=None):
    """GET request to TMDB API."""
    if not TMDB_API_KEY:
        return None
    url = f"https://api.themoviedb.org/3{endpoint}"
    headers = {"Content-Type": "application/json"}
    if params is None:
        params = {}
    params["api_key"] = TMDB_API_KEY
    return _api_request_with_retry(requests.get, url, headers, params=params)


def fetch_tmdb_details(media_type, tmdb_id):
    """Fetch TMDB details with caching. Returns dict or None.
    All required fields (type, networks, number_of_episodes, number_of_seasons, status)
    are present in the base TMDB endpoint response — no append_to_response needed."""
    cache_key = f"{media_type}:{tmdb_id}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    endpoint = f"/tv/{tmdb_id}" if media_type == "show" else f"/movie/{tmdb_id}"

    try:
        data = tmdb_get(endpoint)
        _tmdb_cache[cache_key] = data
        return data
    except Exception as e:
        log.debug(f"TMDB fetch failed for {media_type} {tmdb_id}: {e}")
        _tmdb_cache[cache_key] = None
        return None



def check_tmdb_filters(media_type, tmdb_id, title):
    """Check TMDB-based filters (episode count, show status, show type, networks, season count,
    original language). Content rating is handled upstream using Trakt data — no TMDB call needed for it.
    Returns (passed, skip_reason) tuple."""
    if not TMDB_API_KEY:
        return True, None

    has_tmdb_filters = (
        TMDB_MAX_EPISODES > 0
        or TMDB_ALLOWED_SHOW_STATUS
        or TMDB_EXCLUDE_SHOW_TYPES
        or TMDB_ALLOWED_NETWORKS
        or TMDB_MAX_SEASONS > 0
        or TMDB_ORIGINAL_LANGUAGE
    )
    if not has_tmdb_filters:
        return True, None

    tmdb_data = fetch_tmdb_details(media_type, tmdb_id)
    if not tmdb_data:
        return True, None  # Gracefully skip if TMDB data unavailable

    # Episode count filter (shows only)
    if media_type == "show" and TMDB_MAX_EPISODES > 0:
        episode_count = tmdb_data.get("number_of_episodes", 0)
        if episode_count > TMDB_MAX_EPISODES:
            log.debug(f"Skipping '{title}' — {episode_count} episodes > {TMDB_MAX_EPISODES}")
            return False, "skipped_too_many_episodes"

    # Show status filter (shows only)
    if media_type == "show" and TMDB_ALLOWED_SHOW_STATUS:
        status = tmdb_data.get("status", "")
        if status and status not in TMDB_ALLOWED_SHOW_STATUS:
            log.debug(f"Skipping '{title}' — status '{status}' not in allowed: {TMDB_ALLOWED_SHOW_STATUS}")
            return False, "skipped_show_status"

    # Show type filter (shows only) — e.g. Scripted, Miniseries, Documentary, Reality, News, Talk Show
    if media_type == "show" and TMDB_EXCLUDE_SHOW_TYPES:
        show_type = tmdb_data.get("type", "")
        if show_type and show_type in TMDB_EXCLUDE_SHOW_TYPES:
            log.debug(f"Skipping '{title}' — show type '{show_type}' is excluded")
            return False, "skipped_show_type"

    # Networks filter (shows only) — pass through if network data is absent
    if media_type == "show" and TMDB_ALLOWED_NETWORKS:
        networks = [n.get("name", "") for n in tmdb_data.get("networks", [])]
        if networks and not any(n in TMDB_ALLOWED_NETWORKS for n in networks):
            log.debug(f"Skipping '{title}' — no allowed network in: {networks}")
            return False, "skipped_network"

    # Season count filter (shows only) — 0 seasons (unknown) passes through
    if media_type == "show" and TMDB_MAX_SEASONS > 0:
        season_count = tmdb_data.get("number_of_seasons", 0)
        if season_count > TMDB_MAX_SEASONS:
            log.debug(f"Skipping '{title}' — {season_count} seasons > {TMDB_MAX_SEASONS}")
            return False, "skipped_too_many_seasons"

    # Original language filter (shows AND movies) — checks TMDB original_language field.
    # More accurate than TRAKT_LANGUAGES which reflects metadata/availability language.
    if TMDB_ORIGINAL_LANGUAGE:
        orig_lang = tmdb_data.get("original_language", "")
        if orig_lang and orig_lang not in TMDB_ORIGINAL_LANGUAGE:
            log.debug(f"Skipping '{title}' — original language '{orig_lang}' not in allowed: {TMDB_ORIGINAL_LANGUAGE}")
            return False, "skipped_original_language"

    return True, None


# ============================================================
# DATABASE
# ============================================================
def init_db():
    """Initialize SQLite database with Trakt discovery tables."""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA journal_mode=WAL")

            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS trakt_tokens (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    access_token TEXT NOT NULL,
                    refresh_token TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS trakt_discovered (
                    media_type TEXT NOT NULL,
                    trakt_id INTEGER NOT NULL,
                    tmdb_id INTEGER,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    rating REAL,
                    PRIMARY KEY (media_type, trakt_id, source)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS trakt_request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    media_type TEXT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL,
                    requested_at TEXT NOT NULL
                )
            """)
            conn.commit()
            return conn
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                wait_time = 0.5 * (attempt + 1)
                log.warning(f"Database locked, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                log.error(f"Database initialization failed: {e}")
                raise


# ============================================================
# OAUTH - DEVICE CODE FLOW
# ============================================================
def save_tokens(conn, token_data):
    """Save OAuth tokens to database."""
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(seconds=token_data["expires_in"])).isoformat()
    now = now.isoformat()

    conn.execute("""
        INSERT OR REPLACE INTO trakt_tokens (id, access_token, refresh_token, expires_at, created_at)
        VALUES (1, ?, ?, ?, ?)
    """, (token_data["access_token"], token_data["refresh_token"], expires_at, now))
    conn.commit()
    log.info(f"Tokens saved (expires: {expires_at})")


def load_tokens(conn):
    """Load OAuth tokens from database. Returns dict or None."""
    row = conn.execute("SELECT access_token, refresh_token, expires_at FROM trakt_tokens WHERE id = 1").fetchone()
    if row:
        return {"access_token": row[0], "refresh_token": row[1], "expires_at": row[2]}
    return None


def refresh_access_token(conn, refresh_token):
    """Refresh the Trakt access token."""
    log.info("Refreshing Trakt access token...")
    data = {
        "refresh_token": refresh_token,
        "client_id": TRAKT_CLIENT_ID,
        "client_secret": TRAKT_CLIENT_SECRET,
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
        "grant_type": "refresh_token",
    }
    try:
        resp = requests.post(f"{TRAKT_BASE_URL}/oauth/token", json=data, timeout=30)
        resp.raise_for_status()
        token_data = resp.json()
        save_tokens(conn, token_data)
        log.info("Token refreshed successfully")
        return token_data["access_token"]
    except requests.exceptions.RequestException as e:
        log.error(f"Token refresh failed: {e}")
        _send_alert_webhook(
            f"Trakt token refresh FAILED: {e}\n"
            "If the token is expired, re-authenticate with:\n"
            "  docker exec media-automation python -u /app/trakt_discovery.py auth"
        )
        return None


def get_valid_token(conn):
    """Get a valid access token, refreshing if needed. Cached per process run."""
    # Return cached token if still fresh
    if _token_cache.get("token") and _token_cache.get("cached_at"):
        age = (datetime.now(timezone.utc) - _token_cache["cached_at"]).total_seconds()
        if age < TOKEN_CACHE_TTL_SECONDS:
            return _token_cache["token"]

    tokens = load_tokens(conn)
    if not tokens:
        log.error("No Trakt tokens found. Run: docker exec media-automation python -u /app/trakt_discovery.py auth")
        return None

    expires_at = datetime.fromisoformat(tokens["expires_at"])
    now = datetime.now(timezone.utc)
    days_left = (expires_at - now).days

    # Log-only expiry warnings (routine — not actionable failures, no webhook)
    # Thresholds tuned for Trakt's 7-day access token lifecycle
    if 0 < days_left <= 1:
        log.error(f"Trakt token expires in {days_left} days — auto-refresh will attempt soon")
    elif days_left <= 3:
        log.warning(f"Trakt token expires in {days_left} days — will auto-refresh soon")

    # Refresh if within 2 days of expiry (tuned for Trakt's 7-day access token lifecycle)
    if now >= expires_at - timedelta(days=2):
        refreshed = refresh_access_token(conn, tokens["refresh_token"])
        if refreshed:
            _token_cache["token"] = refreshed
            _token_cache["cached_at"] = datetime.now(timezone.utc)
            return refreshed
        if now >= expires_at:
            msg = (
                "Trakt token EXPIRED and refresh FAILED. Re-authenticate with:\n"
                "  docker exec media-automation python -u /app/trakt_discovery.py auth"
            )
            log.error(msg)
            _send_alert_webhook(msg)
            return None

    token = tokens["access_token"]
    _token_cache["token"] = token
    _token_cache["cached_at"] = datetime.now(timezone.utc)
    return token


def init_device_auth(conn):
    """Start Trakt device code OAuth flow."""
    if not TRAKT_CLIENT_ID:
        log.error("TRAKT_CLIENT_ID not set. Create an app at https://trakt.tv/oauth/applications")
        return False

    log.info("Starting Trakt device code authentication...")
    data = {"client_id": TRAKT_CLIENT_ID}
    try:
        resp = requests.post(f"{TRAKT_BASE_URL}/oauth/device/code", json=data, timeout=30)
        resp.raise_for_status()
        device_data = resp.json()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to get device code: {e}")
        return False

    user_code = device_data["user_code"]
    verification_url = device_data["verification_url"]
    device_code = device_data["device_code"]
    interval = device_data.get("interval", 5)
    expires_in = device_data["expires_in"]

    print(f"\n{'='*50}")
    print(f"  Go to: {verification_url}")
    print(f"  Enter code: {user_code}")
    print(f"  (Code expires in {expires_in // 60} minutes)")
    print(f"{'='*50}\n")

    return poll_device_token(conn, device_code, interval, expires_in)


def poll_device_token(conn, device_code, interval, expires_in):
    """Poll for device token approval."""
    data = {
        "code": device_code,
        "client_id": TRAKT_CLIENT_ID,
        "client_secret": TRAKT_CLIENT_SECRET,
    }
    deadline = time.time() + expires_in

    while time.time() < deadline:
        time.sleep(interval)
        try:
            resp = requests.post(f"{TRAKT_BASE_URL}/oauth/device/token", json=data, timeout=30)

            if resp.status_code == 200:
                token_data = resp.json()
                save_tokens(conn, token_data)
                log.info("Authentication successful!")
                return True
            elif resp.status_code == 400:
                # Pending - user hasn't approved yet
                continue
            elif resp.status_code == 404:
                log.error("Invalid device code")
                return False
            elif resp.status_code == 409:
                log.error("Code already approved")
                return False
            elif resp.status_code == 410:
                log.error("Code expired")
                return False
            elif resp.status_code == 418:
                log.error("User denied the request")
                return False
            elif resp.status_code == 429:
                # Slow down
                interval += 1
                continue
        except requests.exceptions.RequestException as e:
            log.warning(f"Poll error: {e}")
            continue

    log.error("Device code expired before approval")
    return False


# ============================================================
# DISCOVERY LOGIC
# ============================================================
def fetch_list(conn, list_type, media_type):
    """Fetch a Trakt list. Returns list of items with extended info."""
    # Build endpoint
    # 'recommended' and 'watchlist' require auth
    auth_required = list_type in ("recommended", "watchlist")

    if list_type == "watchlist":
        endpoint = f"/users/me/watchlist/{media_type}s"
        params = {"extended": "full"}
    elif list_type == "recommended":
        endpoint = f"/recommendations/{media_type}s"
        params = {"extended": "full"}
    else:
        # trending, popular, anticipated — server-side year filter is safe here
        endpoint = f"/{media_type}s/{list_type}"
        params = {"extended": "full", "limit": str(TRAKT_ITEMS_PER_LIST)}
        if TRAKT_YEARS:
            params["years"] = TRAKT_YEARS

    # Add remaining filters as query params (applied to all list types)
    # Note: years is intentionally NOT sent to recommended/watchlist — those are
    # personalised lists where the app-level year filter (with premium bypass) must
    # own the gating. Sending years server-side would prevent bypass from ever firing.
    if TRAKT_GENRES:
        params["genres"] = TRAKT_GENRES
    if TRAKT_LANGUAGES:
        params["languages"] = TRAKT_LANGUAGES

    items = trakt_get(endpoint, params=params, auth_required=auth_required, conn=conn)
    if items is None:
        log.warning(f"Failed to fetch {list_type} {media_type}s")
        return []

    if not isinstance(items, list):
        log.warning(f"Unexpected response for {list_type} {media_type}s: {type(items)}")
        return []

    return items


def extract_item_info(item, media_type, list_type):
    """Extract standardized info from a Trakt list item."""
    # Trakt wraps items differently depending on list type
    # trending: {"watchers": N, "show": {...}} or {"watchers": N, "movie": {...}}
    # popular: [{"title": ..., "ids": {...}}] (direct)
    # anticipated: {"list_count": N, "show": {...}} or {"list_count": N, "movie": {...}}
    # recommended: [{"title": ..., "ids": {...}}] (direct)
    # watchlist: {"show": {...}} or {"movie": {...}}

    media_key = "show" if media_type == "show" else "movie"

    if media_key in item:
        media = item[media_key]
    elif "title" in item and "ids" in item:
        # Direct item (popular, recommended)
        media = item
    else:
        return None

    ids = media.get("ids", {})
    return {
        "trakt_id": ids.get("trakt"),
        "tmdb_id": ids.get("tmdb"),
        "title": media.get("title", "Unknown"),
        "year": media.get("year"),
        "rating": media.get("rating", 0),
        "votes": media.get("votes", 0),
        "genres": [g.lower() for g in media.get("genres", [])],
        "certification": media.get("certification"),  # e.g. "TV-MA", "PG-13" — present in extended=full
    }


def is_already_discovered(conn, media_type, trakt_id):
    """Check if an item has been permanently handled or is within its Seerr recheck window.

    Permanently blocked: actions in TERMINAL_ACTIONS (requested, skipped_watched, skipped_no_tmdb).
    Time-gated: skipped_exists — re-evaluated after TRAKT_SEERR_RECHECK_DAYS so that content
    deleted by auto-purge tools (e.g. Jellysweep) can be re-discovered and re-requested.
    Filter-based skips (skipped_year, skipped_genre, etc.) are never blocking — they are
    re-evaluated each run so config changes take effect without a DB reset.
    """
    # Phase 1: permanently terminal actions
    placeholders = ",".join("?" * len(TERMINAL_ACTIONS))
    row = conn.execute(
        f"SELECT 1 FROM trakt_discovered WHERE media_type = ? AND trakt_id = ? AND action IN ({placeholders})",
        (media_type, trakt_id, *TERMINAL_ACTIONS)
    ).fetchone()
    if row:
        return True

    # Phase 2: skipped_exists within recheck window
    if TRAKT_SEERR_RECHECK_DAYS == 0:
        # 0 = permanent — never re-check skipped_exists
        row = conn.execute(
            "SELECT 1 FROM trakt_discovered WHERE media_type = ? AND trakt_id = ? AND action = 'skipped_exists'",
            (media_type, trakt_id)
        ).fetchone()
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=TRAKT_SEERR_RECHECK_DAYS)).isoformat()
        row = conn.execute(
            "SELECT 1 FROM trakt_discovered WHERE media_type = ? AND trakt_id = ? AND action = 'skipped_exists' AND discovered_at > ?",
            (media_type, trakt_id, cutoff)
        ).fetchone()
    return row is not None


def check_seerr_status(media_type, tmdb_id):
    """Check if media already exists or is requested in Seerr. Returns True if available/requested."""
    endpoint = f"/{'tv' if media_type == 'show' else 'movie'}/{tmdb_id}"
    try:
        data = seerr_get(endpoint)
        if data is None:
            return False
        # Seerr mediaInfo.status: 1=unknown, 2=pending, 3=processing, 4=partially_available, 5=available
        media_info = data.get("mediaInfo")
        if media_info:
            status = media_info.get("status", 1)
            if status >= 2:
                return True
        return False
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return False
        log.warning(f"Seerr status check failed for {media_type} TMDB:{tmdb_id}: {e}")
        return False
    except Exception as e:
        log.warning(f"Seerr status check error for {media_type} TMDB:{tmdb_id}: {e}")
        return False


def request_via_seerr(media_type, tmdb_id, title):
    """Request content through Seerr. Returns True if successful."""
    if media_type == "show":
        payload = {"mediaType": "tv", "mediaId": tmdb_id, "seasons": [1]}
    else:
        payload = {"mediaType": "movie", "mediaId": tmdb_id}

    if SEERR_USER_ID:
        payload["userId"] = SEERR_USER_ID

    if DRY_RUN:
        log.info(f"[DRY RUN] Would request {media_type} '{title}' (TMDB:{tmdb_id}) via Seerr")
        return True

    try:
        result = seerr_post("/request", payload)
        if result:
            season_info = " (Season 1)" if media_type == "show" else ""
            log.info(f"Requested {media_type} '{title}'{season_info} via Seerr (TMDB:{tmdb_id})")
            return True
        return False
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 409:
            log.info(f"Already requested: '{title}' (TMDB:{tmdb_id})")
            return False
        log.error(f"Seerr request failed for '{title}': {e}")
        return False
    except Exception as e:
        log.error(f"Seerr request error for '{title}': {e}")
        return False


def record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, action, rating):
    """Record a discovered item in the database."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO trakt_discovered (media_type, trakt_id, tmdb_id, title, source, discovered_at, action, rating)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (media_type, trakt_id, tmdb_id, title, source, now, action, rating))
    conn.commit()


def record_request(conn, media_type, tmdb_id, title, source):
    """Record a successful request in the log."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO trakt_request_log (media_type, tmdb_id, title, source, requested_at)
        VALUES (?, ?, ?, ?, ?)
    """, (media_type, tmdb_id, title, source, now))
    conn.commit()


def fetch_watched_ids(conn):
    """Fetch Trakt IDs of all watched shows and movies. Returns dict of sets keyed by media type."""
    watched = {"show": set(), "movie": set()}
    for media_type in ("show", "movie"):
        endpoint = f"/users/me/watched/{media_type}s"
        items = trakt_get(endpoint, auth_required=True, conn=conn)
        if not items or not isinstance(items, list):
            log.warning(f"Could not fetch watched {media_type}s from Trakt")
            continue
        for item in items:
            media = item.get(media_type, {})
            trakt_id = media.get("ids", {}).get("trakt")
            if trakt_id:
                watched[media_type].add(trakt_id)
        log.info(f"Loaded {len(watched[media_type])} watched {media_type}s from Trakt history")
    return watched


def process_discovered_item(conn, item, media_type, source, request_count, max_requests, watched_ids=None):
    """Process a single discovered item through the filter pipeline.
    Returns updated request_count."""
    info = extract_item_info(item, media_type, source)
    if not info or not info["trakt_id"]:
        return request_count

    title = info["title"]
    trakt_id = info["trakt_id"]
    tmdb_id = info["tmdb_id"]
    rating = info["rating"]
    votes = info["votes"]

    # Skip if no TMDB ID
    if not tmdb_id:
        log.debug(f"Skipping '{title}' — no TMDB ID")
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_no_tmdb", rating)
        return request_count

    # Skip if already discovered
    if is_already_discovered(conn, media_type, trakt_id):
        return request_count

    # Skip if already watched on Trakt
    if watched_ids and trakt_id in watched_ids.get(media_type, set()):
        log.debug(f"Skipping '{title}' — already watched on Trakt")
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_watched", rating)
        return request_count

    # Skip if below rating threshold
    if rating and rating < TRAKT_MIN_RATING:
        log.debug(f"Skipping '{title}' — rating {rating:.1f} < {TRAKT_MIN_RATING}")
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_rating", rating)
        return request_count

    # Skip if below vote threshold
    if votes and votes < TRAKT_MIN_VOTES:
        log.debug(f"Skipping '{title}' — {votes} votes < {TRAKT_MIN_VOTES}")
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_votes", rating)
        return request_count

    # Skip if outside year range
    year = info.get("year")
    if year:
        if TRAKT_YEAR_MIN and year < TRAKT_YEAR_MIN:
            if qualifies_for_premium_bypass(source, rating, "year"):
                log.debug(f"'{title}' ({year}) — year below threshold but premium bypass applies [{rating:.1f}]")
            else:
                log.debug(f"Skipping '{title}' — year {year} < {TRAKT_YEAR_MIN}")
                record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_year", rating)
                return request_count
        if TRAKT_YEAR_MAX and year > TRAKT_YEAR_MAX:
            if qualifies_for_premium_bypass(source, rating, "year"):
                log.debug(f"'{title}' ({year}) — year above threshold but premium bypass applies [{rating:.1f}]")
            else:
                log.debug(f"Skipping '{title}' — year {year} > {TRAKT_YEAR_MAX}")
                record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_year", rating)
                return request_count

    # Skip if genre is excluded
    if TRAKT_EXCLUDE_GENRES and info.get("genres"):
        matched_genres = [g for g in info["genres"] if g in TRAKT_EXCLUDE_GENRES]
        if matched_genres:
            log.debug(f"Skipping '{title}' — excluded genre(s): {', '.join(matched_genres)}")
            record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_genre", rating)
            return request_count

    # Content rating filter — uses Trakt certification field, no TMDB key required
    if FILTER_CONTENT_RATINGS or FILTER_EXCLUDE_CONTENT_RATINGS:
        cert = info.get("certification")
        if cert:
            if FILTER_CONTENT_RATINGS and cert not in FILTER_CONTENT_RATINGS:
                log.debug(f"Skipping '{title}' — content rating '{cert}' not in allowed: {FILTER_CONTENT_RATINGS}")
                record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_content_rating", rating)
                return request_count
            if FILTER_EXCLUDE_CONTENT_RATINGS and cert in FILTER_EXCLUDE_CONTENT_RATINGS:
                log.debug(f"Skipping '{title}' — content rating '{cert}' is excluded: {FILTER_EXCLUDE_CONTENT_RATINGS}")
                record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_content_rating", rating)
                return request_count
        else:
            log.debug(f"'{title}' — no certification in Trakt data, content rating filter skipped")

    # TMDB-based filters (episode count, show status, show type, networks, season count)
    passed, skip_reason = check_tmdb_filters(media_type, tmdb_id, title)
    if not passed:
        if skip_reason == "skipped_show_status" and qualifies_for_premium_bypass(source, rating, "status"):
            log.debug(f"'{title}' — show status filtered but premium bypass applies [{rating:.1f}]")
        else:
            record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, skip_reason, rating)
            return request_count

    # Check if already in Seerr
    if check_seerr_status(media_type, tmdb_id):
        log.debug(f"Skipping '{title}' — already in Seerr")
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_exists", rating)
        return request_count

    # Respect request limit
    if request_count >= max_requests:
        log.info(f"Request limit reached ({max_requests}), stopping")
        return request_count

    # Request via Seerr
    if request_via_seerr(media_type, tmdb_id, title):
        if not DRY_RUN:
            # Don't write "requested" to DB during dry runs — items shown as "Would request"
            # should not be permanently blocked from appearing in subsequent dry runs or live runs.
            record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "requested", rating)
            record_request(conn, media_type, tmdb_id, title, source)
        request_count += 1
        year_str = f" ({info['year']})" if info.get("year") else ""
        rating_str = f" [{rating:.1f}]" if rating else ""
        log.info(f"[{request_count}/{max_requests}] Discovered & requested: '{title}'{year_str}{rating_str} from {source}")
    else:
        record_discovered(conn, media_type, trakt_id, tmdb_id, title, source, "skipped_exists", rating)

    return request_count


def discover_content(conn):
    """Main discovery orchestrator. Loops through configured lists and media types."""
    if not TRAKT_CLIENT_ID:
        log.error("TRAKT_CLIENT_ID not set. Cannot discover content.")
        return

    if not SEERR_URL or not SEERR_API_KEY:
        log.error("SEERR_URL and SEERR_API_KEY required for content requests.")
        return

    media_types = []
    if TRAKT_DISCOVER_SHOWS:
        media_types.append("show")
    if TRAKT_DISCOVER_MOVIES:
        media_types.append("movie")

    if not media_types:
        log.warning("Neither shows nor movies enabled for discovery")
        return

    # Calculate per-type limits
    if TRAKT_MAX_SHOW_REQUESTS or TRAKT_MAX_MOVIE_REQUESTS:
        max_show = TRAKT_MAX_SHOW_REQUESTS if TRAKT_MAX_SHOW_REQUESTS else TRAKT_MAX_REQUESTS_PER_CYCLE
        max_movie = TRAKT_MAX_MOVIE_REQUESTS if TRAKT_MAX_MOVIE_REQUESTS else TRAKT_MAX_REQUESTS_PER_CYCLE
    else:
        # Split evenly (backward compatible)
        enabled_count = len(media_types)
        per_type = TRAKT_MAX_REQUESTS_PER_CYCLE // enabled_count if enabled_count else 0
        max_show = per_type if "show" in media_types else 0
        max_movie = per_type if "movie" in media_types else 0

    if "show" not in media_types:
        max_show = 0
    if "movie" not in media_types:
        max_movie = 0

    type_limits = {"show": max_show, "movie": max_movie}
    type_counts = {"show": 0, "movie": 0}

    log.info(f"Starting Trakt discovery cycle (lists: {TRAKT_LISTS}, types: {media_types}, limits: shows={max_show}, movies={max_movie})")
    if DRY_RUN:
        log.info("[DRY RUN] No actual requests will be made")

    # Clear per-cycle caches
    _tmdb_cache.clear()
    _token_cache.clear()

    # Fetch watch history to skip already-watched content
    watched_ids = fetch_watched_ids(conn)

    for list_type in TRAKT_LISTS:
        # Check if all limits reached
        all_done = all(type_counts[mt] >= type_limits[mt] for mt in media_types)
        if all_done:
            break

        for media_type in media_types:
            if type_counts[media_type] >= type_limits[media_type]:
                continue

            log.info(f"Fetching {list_type} {media_type}s...")
            items = fetch_list(conn, list_type, media_type)
            log.info(f"  Found {len(items)} items")

            for item in items:
                if type_counts[media_type] >= type_limits[media_type]:
                    break
                try:
                    type_counts[media_type] = process_discovered_item(
                        conn, item, media_type, list_type,
                        type_counts[media_type], type_limits[media_type],
                        watched_ids=watched_ids
                    )
                except Exception as e:
                    log.error(f"Error processing item: {e}", exc_info=True)
                    continue

    total = sum(type_counts.values())
    parts = [f"{mt}s: {type_counts[mt]}/{type_limits[mt]}" for mt in media_types]
    log.info(f"Discovery cycle complete: {total} new requests ({', '.join(parts)})")


# ============================================================
# CLI COMMANDS
# ============================================================
def cmd_auth(conn):
    """Run OAuth device code flow."""
    return init_device_auth(conn)


def cmd_reauth(conn):
    """Clear existing token and re-run OAuth flow."""
    conn.execute("DELETE FROM trakt_tokens")
    conn.commit()
    log.info("Cleared existing tokens")
    return init_device_auth(conn)


def cmd_status(conn):
    """Show token validity, discovery stats, recent requests."""
    # Token status
    tokens = load_tokens(conn)
    if tokens:
        expires_at = datetime.fromisoformat(tokens["expires_at"])
        now = datetime.now(timezone.utc)
        days_left = (expires_at - now).days
        if now < expires_at:
            print(f"Token: Valid ({days_left} days until expiry)")
            if days_left <= 1:
                print(f"  !! DANGER: expires in {days_left} days — re-auth may be needed soon")
                print(f"  Run: docker exec media-automation python -u /app/trakt_discovery.py auth")
            elif days_left <= 3:
                print(f"  Warning: expires in {days_left} days (auto-refresh will trigger soon)")
        else:
            print("Token: EXPIRED — run 'auth' to re-authenticate")
            print("  Run: docker exec media-automation python -u /app/trakt_discovery.py auth")
        row = conn.execute("SELECT created_at FROM trakt_tokens WHERE id = 1").fetchone()
        if row and row[0]:
            last_refreshed = row[0][:19].replace("T", " ") + " UTC"
            print(f"  Last refreshed: {last_refreshed}")
    else:
        print("Token: Not configured — run 'auth' to authenticate")
        print("  Run: docker exec media-automation python -u /app/trakt_discovery.py auth")

    # Discovery stats
    row = conn.execute("SELECT COUNT(*) FROM trakt_discovered").fetchone()
    total_discovered = row[0] if row else 0

    row = conn.execute("SELECT COUNT(*) FROM trakt_discovered WHERE action = 'requested'").fetchone()
    total_requested = row[0] if row else 0

    row = conn.execute("SELECT COUNT(*) FROM trakt_request_log").fetchone()
    total_logged = row[0] if row else 0

    print(f"\nDiscovery Stats:")
    print(f"  Total discovered: {total_discovered}")
    print(f"  Total requested:  {total_requested}")
    print(f"  All-time request log: {total_logged}")

    # Breakdown by action
    rows = conn.execute(
        "SELECT action, COUNT(*) FROM trakt_discovered GROUP BY action ORDER BY COUNT(*) DESC"
    ).fetchall()
    if rows:
        print(f"\n  By action:")
        for action, count in rows:
            print(f"    {action}: {count}")

    # Recent requests (from trakt_discovered — consistent with reset)
    rows = conn.execute(
        "SELECT media_type, title, source, discovered_at FROM trakt_discovered WHERE action = 'requested' ORDER BY discovered_at DESC LIMIT 10"
    ).fetchall()
    if rows:
        print(f"\nRecent Requests:")
        for media_type, title, source, discovered_at in rows:
            ts = discovered_at[:19].replace("T", " ")
            print(f"  [{media_type}] {title} (from {source}) — {ts}")


def cmd_reset(conn):
    """Clear discovered items table for a fresh start."""
    conn.execute("DELETE FROM trakt_discovered")
    conn.commit()
    row = conn.execute("SELECT COUNT(*) FROM trakt_request_log").fetchone()
    log.info(f"Cleared discovered items table. Request log retained ({row[0]} entries).")


def cmd_discover(conn):
    """Run one discovery cycle."""
    discover_content(conn)


# ============================================================
# MAIN
# ============================================================
def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "discover"

    conn = init_db()
    try:
        if command == "auth":
            success = cmd_auth(conn)
            sys.exit(0 if success else 1)
        elif command == "reauth":
            success = cmd_reauth(conn)
            sys.exit(0 if success else 1)
        elif command == "status":
            cmd_status(conn)
        elif command == "reset":
            cmd_reset(conn)
        elif command == "discover":
            cmd_discover(conn)
        else:
            print(f"Unknown command: {command}")
            print("Usage: python trakt_discovery.py [auth|reauth|discover|status|reset]")
            sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
