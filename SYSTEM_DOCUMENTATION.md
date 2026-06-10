# Cascade Media - Complete System Documentation

> Last updated: 2026-03-31
> Purpose: Exhaustive reference for understanding the entire Cascade Media automation system.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Required external configuration](#required-external-configuration)
3. [Architecture](#architecture)
4. [File Inventory](#file-inventory)
5. [scheduler.py - The Orchestrator](#schedulerpy---the-orchestrator)
6. [media_automation.py - Core TV Automation](#media_automationpy---core-tv-automation)
7. [trakt_discovery.py - Content Discovery Engine](#trakt_discoverypy---content-discovery-engine)
8. [Environment Variables - Complete Reference](#environment-variables---complete-reference)
9. [Database Schema](#database-schema)
10. [API Integrations](#api-integrations)
11. [Execution Flows](#execution-flows)
12. [Filter Pipeline](#filter-pipeline)
13. [Docker and Deployment](#docker-and-deployment)

---

## Required external configuration

cascade-media relies on two settings outside its own `.env` to work correctly. Both must be configured before the cascade behaves as designed.

### 1. Sonarr webhook → cascade-media :9191

Sonarr → Settings → Connect → add a new **Webhook**:

- **Name:** cascade-media (anything)
- **URL:** `http://<cascade-media-host>:9191`
- **Method:** POST
- **Triggers:** check **On Series Add** only (the only event cascade-media handles)

Without this, new series in Sonarr never trigger cascade-media's `process_new_series` and the cascade never runs.

### 2. Jellyseerr/Overseerr → "Enable Automatic Search" UNCHECKED

Jellyseerr → Settings → Services → Sonarr → [your Sonarr server] → edit (pencil icon) → **uncheck "Enable Automatic Search"** → Save. Repeat for each Sonarr server (e.g. a 4K-only instance).

**Why this is required.** When Seerr creates a series in Sonarr with `addOptions.searchForMissingEpisodes: true` (the Seerr default), Sonarr immediately queues a `MissingEpisodeSearch` command. That command snapshots every monitored episode ID at queue time — when Sonarr's default makes every episode `monitored=true` — and serially grabs them over ~60–120 seconds. The running command does NOT re-check episode `monitored` state per iteration, and Sonarr returns `409 Conflict` for any `DELETE /api/v3/command/{id}` against a command in `started` status (and commands transition `queued → started` in <1 second). No code-level fix in cascade-media can intercept this in time.

With auto-search disabled at the Seerr layer, Seerr passes `searchForMissingEpisodes: false` when creating the series in Sonarr. No `MissingEpisodeSearch` fires. cascade-media's explicit `SeasonSearch` (target season) and `EpisodeSearch` (preview E01s) at ~T+17 s are the ONLY searches that run, with monitoring already correct before they trigger.

**Cost:** ~15 second delay between Seerr "Request" and first NZB grab (vs. ~2 s with auto-search on). Movies (Radarr) are unaffected — different setting, different code path.

**History.** Initially attempted in cascade-media code: v1.2.2 (bulk PUT + reorder, narrowed window to 250 ms) and v1.2.3 (`cancel_sonarr_auto_search`, DELETE the command on webhook arrival). Both insufficient — v1.2.2 left Sonarr's enumerated snapshot intact; v1.2.3 hit 409 Conflict because the command was already `started` by the time cascade-media's webhook handler completed its `GET /api/v3/command` + targeted `DELETE`. Moved to the Seerr config layer in v1.2.4 (2026-05-25). Validated end-to-end with Rivals (2024) on Jellyseerr v3.2.0.

---

## System Overview

Cascade Media is an automated media management system that:

1. **Discovers** new movies and TV shows via Trakt API (trending, popular, anticipated, recommended, watchlist)
2. **Requests** discovered content through Seerr (which routes to Sonarr/Radarr for downloading)
3. **Manages TV show downloads intelligently** - only downloads Season 1 fully + Episode 1 of other seasons as "previews"
4. **Monitors watch progress** via Jellyfin - when a user watches enough of a season, automatically unlocks the next
5. **Detects active playback** - when a user starts playing a preview Episode 1, immediately unlocks that full season
6. **Boosts download priority** in SABnzbd for actively-watched content

### The "Cascade" Philosophy

Instead of downloading entire series upfront, the system creates a cascade effect:
- Full Season 1 downloads immediately
- Episode 1 of every other season downloads as a preview
- When a user watches ~75% of a season, the next season auto-unlocks
- When a user starts playing any preview E01, that full season unlocks immediately
- This continues cascading through all seasons as the user watches

---

## Architecture

```
                    +-----------------+
                    |   Trakt API     |  (Content Discovery)
                    +--------+--------+
                             |
                    +--------v--------+
                    | trakt_discovery  |  Discovers and filters content
                    |     .py          |  Requests via Seerr
                    +--------+--------+
                             |
                    +--------v--------+
                    |     Seerr       |  (Request Management)
                    +--------+--------+
                             |
               +-------------+-------------+
               |                           |
      +--------v--------+        +--------v--------+
      |     Sonarr      |        |     Radarr      |
      |   (TV Shows)    |        |    (Movies)     |
      +--------+--------+        +-----------------+
               |
      +--------v--------+
      | media_automation |  Monitors, unlocks, boosts
      |       .py        |
      +--------+--------+
               |
     +---------+---------+----------+
     |                   |          |
+----v-----+    +--------v---+  +--v----------+
| Jellyfin |    |  SABnzbd   |  |   Sonarr    |
| (Watch   |    | (Download  |  | (Episode    |
|  Progress|    |  Priority) |  |  Monitoring)|
+----------+    +------------+  +-------------+


SCHEDULER (scheduler.py) orchestrates everything:
  - Main poll loop: every 15 min -> media_automation.py
  - Playback check: every 45 sec -> media_automation.py playback
  - Webhook listener: port 9191 -> media_automation.py webhook <id>
  - Trakt discovery: daily at configured time -> trakt_discovery.py discover
  - Startup catchup: once -> media_automation.py catchup
```

---

## File Inventory

| File | Lines | Purpose |
|------|-------|---------|
| `scheduler.py` | 319 | Orchestrator - scheduling, webhook server, concurrency |
| `media_automation.py` | 1418 | TV show lifecycle - monitoring, unlocking, queue management |
| `trakt_discovery.py` | 1284 | Content discovery - Trakt + TMDB + Seerr with 13-stage filter |
| `docker-compose.yml` | 18 | Docker Compose service definition |
| `Dockerfile` | 12 | Container build (python:3.11-slim + requests + tzdata) |
| `.env.example` | ~115 | Environment variable template |
| `.github/workflows/docker-build.yml` | 35 | CI/CD for GHCR publishing |
| `templates/cascade-media.xml` | ~200 | Unraid Community Applications template |
| `README.md` | ~600 | User documentation |
| `logo.svg` | 1 | 100x100 SVG icon |
| `LICENSE` | 21 | MIT License |
| `data/media_automation.db` | - | SQLite database (7 tables, WAL mode) |

---

## scheduler.py - The Orchestrator

### Purpose
Entry point for the Docker container. Manages scheduling, concurrency, and the webhook HTTP server.

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `RUN_INTERVAL_MINUTES` | 15 | Main polling interval |
| `RUN_CATCHUP_ON_START` | true | Run catchup on startup |
| `WEBHOOK_PORT` | 9191 | Sonarr webhook listener port |
| `SCRIPT_TIMEOUT_MINUTES` | 30 | Poll/webhook script timeout |
| `PLAYBACK_CHECK_INTERVAL` | 45 | Playback check interval (seconds) |
| `PLAYBACK_SCRIPT_TIMEOUT` | 600 | Playback script timeout (seconds) |
| `TRAKT_DISCOVERY_ENABLED` | false | Enable daily Trakt discovery |
| `TRAKT_DISCOVERY_TIME` | 00:00 | Trakt run time (HH:MM, 24h) |
| `TRAKT_DISCOVERY_TZ` | UTC | IANA timezone for scheduling |
| `TRAKT_SCRIPT_TIMEOUT` | 300 | Trakt script timeout (seconds) |

### Threading Architecture

| Thread | Type | Lock | Purpose |
|--------|------|------|---------|
| Main | Main thread | `poll_lock` (non-blocking) | 15-min polling loop |
| Webhook Server | Daemon | N/A | HTTPServer on port 9191 |
| Playback Check | Daemon | `playback_lock` (non-blocking) | 45-sec playback detection |
| Trakt Discovery | Daemon | `trakt_lock` (non-blocking) | Daily clock-based discovery |
| Webhook Executor | ThreadPool(3) | `webhook_lock` (blocking) | Sequential webhook processing |

### Script Invocation Matrix

| Script | Command | Trigger | Timeout |
|--------|---------|---------|---------|
| `media_automation.py` | (none) | Main loop every 15m | 1800s |
| `media_automation.py` | `catchup` | Startup (once) | 1800s |
| `media_automation.py` | `playback` | Every 45s | 600s |
| `media_automation.py` | `webhook <id>` | Sonarr SeriesAdd event | 1800s |
| `trakt_discovery.py` | `discover` | Daily at TRAKT_DISCOVERY_TIME | 300s |

### Webhook HTTP Server

- **POST /**: Sonarr webhooks
  - `SeriesAdd`: Queues to thread pool
  - `Test`: Returns 200
  - `Grab`: Logged, deferred to next poll
  - Unknown: Logged and ignored
- **GET /**: Health check returns `{"status": "running"}`

---

## media_automation.py - Core TV Automation

### Purpose
TV show download lifecycle: initial setup, watch progress monitoring, playback detection, season unlocking, priority boosting.

**This script is TV-only.** Movies go Trakt -> Seerr -> Radarr directly.

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SONARR_URL` | "" | Sonarr API base URL |
| `SONARR_API_KEY` | "" | Sonarr API key |
| `JELLYFIN_URL` | "" | Jellyfin API base URL |
| `JELLYFIN_API_KEY` | "" | Jellyfin API key |
| `SEERR_URL` | "" | Seerr API base URL |
| `SEERR_API_KEY` | "" | Seerr API key |
| `SABNZBD_URL` | "" | SABnzbd API base URL |
| `SABNZBD_API_KEY` | "" | SABnzbd API key |
| `JELLYFIN_USER_IDS` | [] | Comma-separated Jellyfin UUIDs |
| `WATCH_THRESHOLD` | 0.75 | Season watch % to trigger unlock (0-1) |
| `NEW_SERIES_LOOKBACK_HOURS` | 24 | Hours to scan for new series |
| `SABNZBD_QUEUE_WAIT_SECONDS` | 120 | Wait for downloads to appear in queue |
| `DB_PATH` | /data/media_automation.db | Database location |
| `DRY_RUN` | false | Log without making changes |
| `LOG_LEVEL` | INFO | Logging verbosity |

### CLI Commands

| Command | Purpose |
|---------|---------|
| (none) | Run all polling tasks |
| `catchup` | Process all existing series (one-time) |
| `webhook <id>` | Process single series by Sonarr ID |
| `playback` | Check active Jellyfin sessions |
| `reprocess <id>` | Clear and reprocess a series |
| `list` | Display all processed series |

### Core Task Flows

#### Task 1: Initial Monitoring for New Series
1. Fetch all Sonarr series, filter by lookback window
2. For each new series:
   - Query Seerr for requested seasons
   - Determine target: Seerr request > existing files > Season 1
   - Monitor: ALL episodes for target, E01 only for others (bulk `PUT /episode/monitor`)
   - Wait 15s + re-apply monitoring (Sonarr's background tasks may re-monitor)
   - Search: SeasonSearch for targets, EpisodeSearch for E01s
   - 3x cleanup passes (10s, 20s, 30s)
   - Record unlocked seasons

> **REQUIRED setup for this flow to work correctly:** Jellyseerr/Overseerr must have "Enable Automatic Search" **unchecked** on each Sonarr server. Otherwise Sonarr fires its own `MissingEpisodeSearch` at series-add time and grabs every episode of every season before cascade-media's monitoring can take effect. See "Setup → Required external configuration" below for the why and the history. Validated 2026-05-25 with Rivals (2024) on Jellyseerr v3.2.0.

#### `determine_target_season` Priority
1. Seerr request with specific seasons -> minimum requested
2. Seerr "remaining" request -> lowest season without files
3. Seerr all seasons -> Season 1 only (prevents mass download)
4. No Seerr, files exist -> lowest season with files
5. No data -> Season 1

#### Task 2: Watch Progress Monitoring
- For each Jellyfin user: query watched episodes
- Calculate season progress: `watched / total`
- If >= 75% and next not unlocked: monitor, search, boost, record

#### Task 3: Playback Detection
- Poll Jellyfin sessions every 45s
- User watching E01 of locked season -> unlock immediately
- Boost with force_e02=True (E02=Force priority, E03+=High)

#### Task 4: SABnzbd Priority Boosting
- Match Sonarr queue -> SABnzbd queue by nzo_id
- Set priority: High(1) or Force(2) for E02
- Retry up to 3 times

#### Task 5: Database Cleanup
- Remove entries for series deleted from Sonarr

#### Task 6: Queue Cleanup
- Cancel downloads for unmonitored episodes

### API Retry Logic
All calls use `_api_request_with_retry()`:
- 429: Wait Retry-After, retry
- 5xx: Exponential backoff (2^attempt)
- Timeout/Connection: Retry up to 3 times

---

## trakt_discovery.py - Content Discovery Engine

### Purpose
Discover movies and TV shows from Trakt, filter through 13-stage pipeline, request via Seerr.

### Environment Variables

#### Core
| Variable | Default | Purpose |
|----------|---------|---------|
| `TRAKT_CLIENT_ID` | "" | OAuth client ID |
| `TRAKT_CLIENT_SECRET` | "" | OAuth client secret |
| `TRAKT_DISCOVER_SHOWS` | true | Discover TV shows |
| `TRAKT_DISCOVER_MOVIES` | true | Discover movies |
| `TRAKT_LISTS` | recommended,watchlist,trending,popular,anticipated | Lists to process |
| `TRAKT_MIN_RATING` | 7.0 | Minimum rating |
| `TRAKT_MIN_VOTES` | 100 | Minimum votes |
| `TRAKT_YEARS` | "" | Year range: `2020-2026` |
| `TRAKT_GENRES` | "" | Genre inclusion: `drama,comedy` |
| `TRAKT_EXCLUDE_GENRES` | "" | Genre exclusion |
| `TRAKT_LANGUAGES` | en | Languages: `en,es` |
| `TRAKT_MAX_REQUESTS_PER_CYCLE` | 10 | Total per cycle |
| `TRAKT_MAX_SHOW_REQUESTS` | 0 | Per-type (0=use cycle) |
| `TRAKT_MAX_MOVIE_REQUESTS` | 0 | Per-type (0=use cycle) |
| `TRAKT_ITEMS_PER_LIST` | 20 | Items per list |

#### Cross-List Priority
| Variable | Default | Purpose |
|----------|---------|---------|
| `TRAKT_CROSS_LIST_PRIORITY` | true | Two-pass discovery |
| `TRAKT_CROSS_LIST_SOURCES` | recommended,watchlist | Source lists |
| `TRAKT_CROSS_LIST_TARGETS` | trending | Target lists |

#### TMDB Filters (require TMDB_API_KEY)
| Variable | Default | Purpose |
|----------|---------|---------|
| `TMDB_API_KEY` | "" | Enables advanced filters |
| `TMDB_MAX_EPISODES` | 0 | Max episodes (0=disabled) |
| `TMDB_MAX_SEASONS` | 0 | Max seasons (0=disabled) |
| `TMDB_ALLOWED_SHOW_STATUS` | "" | e.g., `Returning Series,Ended` |
| `TMDB_EXCLUDE_SHOW_TYPES` | "" | e.g., `Reality,Talk Show` |
| `TMDB_ALLOWED_NETWORKS` | "" | Network allow-list |
| `TMDB_DISALLOWED_NETWORKS` | "" | Network deny-list (originating network) |
| `TMDB_DISALLOWED_PROVIDERS` | "" | Streaming provider deny-list (shows only, more robust — see below) |
| `TMDB_PROVIDER_REGION` | `US` | ISO 3166-1 country code for watch provider lookup |
| `TMDB_ORIGINAL_LANGUAGE` | "" | e.g., `en,ko` |

#### Content Ratings (uses Trakt cert, no TMDB key needed)
| Variable | Default | Purpose |
|----------|---------|---------|
| `FILTER_CONTENT_RATINGS` | "" | Allow-list: `TV-MA,R` |
| `FILTER_EXCLUDE_CONTENT_RATINGS` | "" | Deny-list |

#### Premium Bypass
| Variable | Default | Purpose |
|----------|---------|---------|
| `TRAKT_PREMIUM_BYPASS_ENABLED` | true | High-rated content bypasses filters |
| `TRAKT_PREMIUM_BYPASS_MIN_RATING` | 8.0 | Rating threshold |
| `TRAKT_PREMIUM_BYPASS_LISTS` | recommended,watchlist | Eligible lists |
| `TRAKT_PREMIUM_BYPASS_FILTERS` | year,status | Bypassable filters |

#### Other
| Variable | Default | Purpose |
|----------|---------|---------|
| `SEERR_USER_ID` | 0 | Request attribution |
| `TRAKT_SEERR_RECHECK_DAYS` | 365 | Recheck skipped_exists |
| `ALERT_WEBHOOK_URL` | "" | Discord/Slack alerts |

### CLI Commands
| Command | Purpose |
|---------|---------|
| `auth` | OAuth device code flow |
| `reauth` | Clear tokens, re-authenticate |
| `status` | Token status + discovery stats |
| `reset` | Clear discovered table |
| `discover` | Run discovery cycle (default) |

### Discovery Modes

**Sequential** (TRAKT_CROSS_LIST_PRIORITY=false): Lists in order, stop at limits.

**Two-Pass** (TRAKT_CROSS_LIST_PRIORITY=true):
- Pass 1: Items on BOTH source AND target lists (highest confidence)
- Pass 2: Remaining items in list order

### Seerr Request Behavior
- TV Shows: Season 1 only (cascade handles rest)
- Movies: Full movie
- 409 = already requested -> skipped_exists

### Discovery State Machine

| Action | Terminal? | Re-evaluated? |
|--------|-----------|---------------|
| `requested` | Yes | Never |
| `skipped_watched` | Yes | Never |
| `skipped_no_tmdb` | Yes | Never |
| `skipped_exists` | Time-gated | After TRAKT_SEERR_RECHECK_DAYS |
| All `skipped_*` filters | No | Every cycle |

---

## Environment Variables - Complete Reference

### Format Examples

| Variable | Format | Example |
|----------|--------|---------|
| `TRAKT_YEARS` | `MIN-MAX` | `2020-2026` |
| `TRAKT_GENRES` | lowercase slugs | `drama,comedy,thriller` |
| `TRAKT_LANGUAGES` | ISO 639-1 | `en,es,fr` |
| `TRAKT_DISCOVERY_TIME` | HH:MM (24h) | `22:00` |
| `TRAKT_DISCOVERY_TZ` | IANA timezone | `America/New_York` |
| `JELLYFIN_USER_IDS` | UUIDs | `a1b2c3d4...,e5f6a7b8...` |
| `FILTER_CONTENT_RATINGS` | certifications | `TV-MA,TV-14,R,PG-13` |

### TMDB Network Names (case-sensitive)

| Service | TMDB Name |
|---------|-----------|
| Netflix | `Netflix` |
| Apple TV+ | `Apple TV` (TMDB dropped the `+` in a 2025 rebrand — same as `HBO Max` → `Max`) |
| HBO/Max | `Max`, `HBO` |
| Amazon | `Amazon Prime Video` |
| Hulu | `Hulu` |
| Disney+ | `Disney+` |
| Peacock | `Peacock` |
| Paramount+ | `Paramount+` |
| AMC | `AMC+`, `AMC` |
| Showtime | `Showtime` |
| Starz | `Starz` |
| FX | `FX`, `FXX` |
| CBS | `CBS` |
| NBC | `NBC` |
| ABC | `ABC (US)` |
| FOX | `FOX` |
| The CW | `The CW` |
| BBC | `BBC One`, `BBC Two`, `BBC Three` |
| Crunchyroll | `Crunchyroll` |

> "HBO Max" was rebranded to "Max" on TMDB.
> "Apple TV+" was rebranded to "Apple TV" on TMDB.

### Networks vs Watch Providers

Two filters target streaming services; they answer different questions and miss different things:

| Filter | Source | What it checks | Matching | Misses |
|--------|--------|----------------|----------|--------|
| `TMDB_DISALLOWED_NETWORKS` | `tv.networks[]` | The show's *originating* broadcast network | Exact, case-sensitive | Shows distributed by streaming services but originating elsewhere (e.g. Bodyguard — BBC One on TMDB, Netflix in reality); also misses rebrand variants |
| `TMDB_DISALLOWED_PROVIDERS` | `tv/{id}/watch/providers` `flatrate` | What's *currently streaming* the show in your region | Case-sensitive `startswith` | Anything not on a subscription tier in `TMDB_PROVIDER_REGION` (rent/buy providers are intentionally ignored) |

The providers filter is **shows only** — movies are deliberately excluded because theatrical releases often have higher-quality non-streaming versions (UHD Blu-ray rips, theatrical remuxes) available even when the streaming version is on a subscription you have. You'd want those higher-quality versions in your library; the providers filter respects that.

Both filters run if both are set. Either skip wins. Skip reasons recorded in `trakt_discovered.action`: `skipped_disallowed_network` and `skipped_disallowed_provider` respectively.

Premium bypass (`TRAKT_PREMIUM_BYPASS_FILTERS`) does **not** apply to either — subscription overlap isn't quality-dependent.

---

## Database Schema

### Tables (stats as of 2026-03-29)

**`processed_series`** (73 rows) - Series configured with monitoring
```sql
sonarr_id INTEGER PRIMARY KEY, title TEXT, processed_at TEXT
```

**`unlocked_seasons`** (141 rows) - Seasons unlocked for download
```sql
sonarr_id INTEGER, season_number INTEGER, unlocked_by TEXT, unlocked_at TEXT
PRIMARY KEY (sonarr_id, season_number)
-- unlocked_by values: "initial_setup", "existing_content", username, "playback:username"
```

**`priority_boosts`** (0 rows) - SABnzbd priority boost tracking
```sql
sonarr_id INTEGER, season_number INTEGER, boosted_at TEXT
PRIMARY KEY (sonarr_id, season_number)
```

**`trakt_tokens`** (1 row) - OAuth tokens
```sql
id INTEGER PRIMARY KEY CHECK(id=1), access_token TEXT, refresh_token TEXT, expires_at TEXT, created_at TEXT
```

**`trakt_discovered`** (254 rows) - Discovery history
```sql
media_type TEXT, trakt_id INTEGER, tmdb_id INTEGER, title TEXT, source TEXT,
discovered_at TEXT, action TEXT, rating REAL
PRIMARY KEY (media_type, trakt_id, source)
```

**`trakt_request_log`** (722 rows) - Audit trail
```sql
id INTEGER PRIMARY KEY AUTOINCREMENT, media_type TEXT, tmdb_id INTEGER,
title TEXT, source TEXT, requested_at TEXT
```

### Configuration
- Journal: WAL (Write-Ahead Logging)
- Busy timeout: 30 seconds
- Init retry: 5 attempts

---

## API Integrations

### Sonarr
| Endpoint | Method | Usage |
|----------|--------|-------|
| `/api/v3/series` | GET | List/get series |
| `/api/v3/episode?seriesId={id}` | GET | Get episodes |
| `/api/v3/episode/{id}` | PUT | Update monitoring |
| `/api/v3/series/{id}` | PUT | Update series |
| `/api/v3/command` | POST | Trigger searches |
| `/api/v3/queue` | GET | Download queue |
| `/api/v3/queue/{id}` | DELETE | Cancel download |

> **Known limitation — "matched by ID" import blocks.** When a completed download's release name doesn't parse to a library title (e.g. `Battlestar.Galactica.2005.S02E01` vs `Battlestar Galactica (2003)`), Sonarr/Radarr can only match it via grab-history ID and refuses to auto-import as a safety measure (`importBlocked`/`importPending`, *"Automatic import is not possible / Manual Import required"*). There is no config toggle to force import-by-ID; today these require a manual import in Activity → Queue. Auto-resolving them from the scheduler (`GET /api/v3/manualimport?downloadId=` → `POST /api/v3/command {"name":"ManualImport"}`) is tracked in `project_backlog.md`.

### Jellyfin
| Endpoint | Usage |
|----------|-------|
| `/Users/{id}` | User info |
| `/Users/{id}/Items` | Watched episodes |
| `/Users/{id}/Items/{id}` | Item details |
| `/Sessions` | Active playback |

### Seerr
| Endpoint | Usage |
|----------|-------|
| `/api/v1/request` | List/create requests |
| `/api/v1/tv/{tmdb_id}` | TV show status |
| `/api/v1/movie/{tmdb_id}` | Movie status |

### SABnzbd
| Mode | Usage |
|------|-------|
| `queue` | Get download queue |
| `queue` + `name=priority` | Set priority |

### Trakt
| Endpoint | Auth | Usage |
|----------|------|-------|
| `/oauth/device/code` | No | Start auth |
| `/oauth/device/token` | No | Poll token |
| `/oauth/token` | No | Refresh token |
| `/recommendations/{type}s` | Yes | Recommendations |
| `/users/me/watchlist/{type}s` | Yes | Watchlist |
| `/users/me/watched/{type}s` | Yes | Watch history |
| `/{type}s/trending` | No | Trending |
| `/{type}s/popular` | No | Popular |
| `/{type}s/anticipated` | No | Anticipated |

### TMDB
| Endpoint | Usage |
|----------|-------|
| `/3/tv/{id}` | Show details |
| `/3/movie/{id}` | Movie details |

---

## Execution Flows

### Flow 1: New Series (Webhook)
```
Sonarr SeriesAdd -> POST :9191 -> webhook_executor
-> media_automation.py webhook <id>
  -> determine_target_season (Seerr > files > S01)
  -> apply_monitoring (bulk PUT: season-level first, then episode bulk)
  -> wait 15s + re-apply
  -> trigger searches (SeasonSearch + EpisodeSearch for E01 previews)
  -> 3x cleanup passes (10s, 20s, 30s) — re-apply + cleanup queue
  -> record unlocked seasons
```
Assumes Jellyseerr's "Enable Automatic Search" is **off** on the Sonarr server — otherwise Sonarr's own `MissingEpisodeSearch` races this flow and over-grabs unrelated seasons.

### Flow 2: Watch Progress
```
Every 15m -> media_automation.py
-> For each user: query Jellyfin watched
-> If season >= 75%: unlock next, search, boost
```

### Flow 3: Playback Detection
```
Every 45s -> media_automation.py playback
-> Poll Jellyfin /Sessions
-> E01 playing of locked season -> unlock, search, boost(force_e02)
```

### Flow 4: Trakt Discovery
```
Daily -> trakt_discovery.py discover
-> Cross-list priority: Pass 1 (source+target), Pass 2 (rest)
-> 13-stage filter pipeline
-> Request via Seerr (S01 for shows, full for movies)
```

---

## Filter Pipeline

### Trakt Discovery (13 stages)

```
1.  Has trakt_id?          --NO--> Skip
2.  Has tmdb_id?           --NO--> Skip (terminal)
3.  Already discovered?    --YES-> Skip
4.  In watch history?      --YES-> Skip (terminal)
5.  Rating >= min?         --NO--> Skip (soft)
6.  Votes >= min?          --NO--> Skip (soft)
7.  Year in range?         --NO--> Premium bypass? / Skip (soft)
8.  Genre excluded?        --YES-> Skip (soft)
9.  Content rating OK?     --NO--> Skip (soft)
10. TMDB filters pass?     --NO--> Premium bypass? / Skip (soft)
11. In Seerr already?      --YES-> Skip (time-gated)
12. Under request limit?   --NO--> Stop
13. Request via Seerr      ------> "requested" (terminal)
```

---

## Docker and Deployment

### Build
- Base: `python:3.11-slim`
- Deps: `requests`, `tzdata`
- Entry: `python scheduler.py`
- Data: `/data` volume

### Compose
- Image: `ghcr.io/normalee1993/cascade-media:latest`
- Restart: `unless-stopped`
- Port: `${WEBHOOK_PORT:-9191}`
- Volume: `./data:/data`

### CI/CD (GitHub Actions)
- Triggers: push to main, push v* tags
- Registry: ghcr.io
- Tags: semver + latest
