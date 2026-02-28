# Media Automation for Unraid

Stop managing your media server manually. This system keeps your library stocked and your storage lean — automatically.

**It discovers content for you.** Connects to Trakt to pull in trending, popular, and personalised recommendations for both TV shows and movies, then requests them through Seerr without any intervention. A multi-stage filter pipeline (rating, votes, year, genre, show status, episode count) ensures only quality content makes it through — and high-rated classics on your watchlist won't get silently dropped just because they're older or finished airing.

**It downloads only what you'll actually watch.** For TV shows, only the requested season is fully downloaded — every other season gets just Episode 1 as a preview. As people watch, the next season downloads automatically in the background. Start playing a preview episode and E02 jumps to Force priority in SABnzbd so it's ready before E01 is done. For movies, Seerr handles the full download as usual — discovery just removes the need to go looking for them yourself.

## How It Works

### When someone requests a show in Seerr

1. Seerr tells Sonarr to add the series (Sonarr monitors ALL episodes by default)
2. This script intercepts the Sonarr webhook and immediately fixes the monitoring:
   - **Requested season(s)**: All episodes downloaded
   - **Every other season**: Only Episode 1 downloaded (as a preview)
3. Unwanted downloads already queued in SABnzbd are automatically cancelled

### As people watch

The script polls Jellyfin every 15 minutes and checks watch progress for each configured user. When someone watches **75% or more** of a season, the next season is automatically unlocked (all episodes monitored + search triggered). Downloads for the new season are set to **High priority** in SABnzbd.

### When someone starts playing a preview episode

Every 45 seconds, the script checks Jellyfin's active sessions. If a user starts playing **Episode 1** of a season that only has the preview downloaded, the full season is automatically unlocked and searched. Downloads are prioritized in SABnzbd: **E02 gets Force priority** (downloads immediately so it's ready when E01 finishes) and **E03+ get High priority**.

### "All seasons" requests

If someone requests every season of a show, the script treats it as: **Season 1 full + Episode 1 of the rest**. This prevents downloading 15 seasons of a show nobody has started watching yet.

## Trakt Content Discovery

Automatically discovers trending, popular, and recommended content via the Trakt API and requests it through Seerr. TV shows request only Season 1 — the core automation's E01-preview logic handles progressive unlocking as people watch.

### What it monitors
- **Trending** — Currently most-watched shows and movies
- **Popular** — All-time most popular
- **Anticipated** — Most anticipated upcoming releases
- **Recommended** — Personalized recommendations based on your Trakt watch history
- **Watchlist** — Your Trakt watchlist

### Filtering pipeline
Each discovered item goes through these filters before being requested:
1. **Already discovered** — Skips items seen in previous cycles
2. **Already watched on Trakt** — Skips your complete watch history (import your Netflix/Amazon/etc. history to Trakt for best results)
3. **Rating threshold** — Skips items below `TRAKT_MIN_RATING` (default: 7.0)
4. **Vote threshold** — Skips items with fewer than `TRAKT_MIN_VOTES` (default: 100)
5. **Year filter** — Skips items outside `TRAKT_YEARS` range (backup for API-level filter that some list types ignore)
6. **Genre exclusion** — Skips items matching `TRAKT_EXCLUDE_GENRES` (e.g., `animation,reality,talk-show`)
7. **Content rating** — `FILTER_CONTENT_RATINGS` (allow-list) and `FILTER_EXCLUDE_CONTENT_RATINGS` (deny-list) use Trakt's certification field directly — no `TMDB_API_KEY` required
8. **TMDB filters** (optional, requires `TMDB_API_KEY`) — Episode count (`TMDB_MAX_EPISODES`), show status (`TMDB_ALLOWED_SHOW_STATUS`), show type (`TMDB_EXCLUDE_SHOW_TYPES`), networks (`TMDB_ALLOWED_NETWORKS`), season count (`TMDB_MAX_SEASONS`), original language (`TMDB_ORIGINAL_LANGUAGE`)
9. **Already in Seerr** — Skips items already requested or available
10. **Request limit** — Stops after per-type limits (`TRAKT_MAX_SHOW_REQUESTS` / `TRAKT_MAX_MOVIE_REQUESTS`)

### Premium Content Bypass

High-rated content from personalised lists (`recommended`, `watchlist` by default) can bypass the year and show status filters. The original `TRAKT_MIN_RATING` floor still applies to everything — a show must clear 7.0 before bypass is even considered.

**Example:** Breaking Bad (2008, Ended, rating 9.3) on your recommended list passes through even with `TRAKT_YEARS=2020-2026` and `TRAKT_ALLOWED_SHOW_STATUS=Returning Series`, because its 9.3 rating clears the 8.0 bypass bar.

| Variable | Default | Description |
|----------|---------|-------------|
| `TRAKT_PREMIUM_BYPASS_ENABLED` | `true` | Toggle the whole feature on/off |
| `TRAKT_PREMIUM_BYPASS_MIN_RATING` | `8.0` | Minimum rating to qualify for bypass (above the normal 7.0 floor) |
| `TRAKT_PREMIUM_BYPASS_LISTS` | `recommended,watchlist` | Comma-separated list sources that get the bypass |
| `TRAKT_PREMIUM_BYPASS_FILTERS` | `year,status` | Which filters are bypassable: `year`, `status`, or `year,status` |

### TMDB Filters — Practical Notes

TMDB filters run at **step 8**, after rating, votes, year, genre, and content rating checks. This means an item must survive all upstream filters before the TMDB lookup is even made. Keep this in mind when setting expectations:

**`TMDB_EXCLUDE_SHOW_TYPES`**
- **`Miniseries` is the most reliable value.** Limited series (Chernobyl, The Night Of, Ironheart, etc.) are common in Trakt trending/anticipated feeds and typically have high enough ratings to reach the TMDB check.
- **`Reality`, `Documentary`, and `Talk Show` are better handled by `TRAKT_EXCLUDE_GENRES`** (e.g., `reality,talk-show,documentary`). Genre exclusion runs earlier in the pipeline (step 6 vs. step 8), requires no TMDB API key, and is more likely to block these shows — because Reality and Talk Show content rarely clears the default 7.0 rating / 100 vote thresholds and gets eliminated before ever reaching the TMDB lookup. Using both is fine; `TRAKT_EXCLUDE_GENRES` acts as the first line of defence.
- **Syntax for values with spaces:** In your `.env` file, multi-word values like `Talk Show` are safe as-is — the entire line is read as a string:
  ```
  TMDB_EXCLUDE_SHOW_TYPES=Miniseries,Reality,Talk Show,Documentary
  ```
  No quoting needed in `.env`. Quoting is only required when passing the value inline in a shell command (e.g. `docker exec`).

**`TMDB_ALLOWED_NETWORKS`**
- This filter works reliably in production. Popular shows on mainstream networks (ABC, NBC, FOX, CBS, Hulu, etc.) do have high ratings and vote counts, so they routinely reach the TMDB check where they'll be blocked if their network isn't on your list.
- Network names must match TMDB exactly, including spacing and capitalisation — e.g. `Apple TV+` not `Apple TV`, `The CW` not `CW`.
- If a show has no network data in TMDB (rare), it passes through rather than being blocked.

**`TMDB_ORIGINAL_LANGUAGE`**
- Use this instead of (or alongside) `TRAKT_LANGUAGES` when you need to filter by actual production language. `TRAKT_LANGUAGES=en` filters at the API level but matches metadata language — a Korean drama with an English localisation can pass through. `TMDB_ORIGINAL_LANGUAGE=en` checks TMDB's `original_language` field, which reflects what language the show or film was actually produced in.
- Uses ISO 639-1 two-letter codes: `en` (English), `ko` (Korean), `fr` (French), `ja` (Japanese), `de` (German), `es` (Spanish), `pt` (Portuguese), `zh` (Chinese).
- This is the **only TMDB filter that applies to both shows and movies** — all other TMDB filters are shows-only.
- If a TMDB record has no `original_language` value (extremely rare), the item passes through rather than being blocked.

### Setup
1. Create a Trakt API application at https://trakt.tv/oauth/applications
   - **Redirect URI:** `urn:ietf:wg:oauth:2.0:oob`
   - **Permissions:** No special permissions needed (leave /checkin and /scrobble unchecked)
2. Add your Client ID and Client Secret to `.env`
3. Rebuild: `docker compose build && docker compose up -d`
4. Authenticate: `docker exec media-automation python -u /app/trakt_discovery.py auth`
5. Visit the URL displayed, enter the code
6. Set `TRAKT_DISCOVERY_ENABLED=true` in `.env` and restart

### Commands
```bash
# Authenticate with Trakt
docker exec media-automation python -u /app/trakt_discovery.py auth

# Check token and discovery stats
docker exec media-automation python -u /app/trakt_discovery.py status

# Dry run (see what would be requested without making changes)
docker exec -e DRY_RUN=true media-automation python -u /app/trakt_discovery.py discover

# Run discovery now
docker exec media-automation python -u /app/trakt_discovery.py discover

# Clear discovered items for a fresh start (keeps request log)
docker exec media-automation python -u /app/trakt_discovery.py reset

# Re-authenticate (clear tokens and start over)
docker exec media-automation python -u /app/trakt_discovery.py reauth
```

---

## Setup

### 1. Configure Sonarr Webhook

In Sonarr, go to **Settings > Connect > Add > Webhook**:
- **Name**: Media Automation
- **URL**: `http://<your-server-ip>:9191`
- **Events**: Check "On Series Add"

### 2. Get API Keys

| Service | Where to find it |
|---------|-----------------|
| Sonarr | Settings > General > API Key |
| Jellyfin | Dashboard > API Keys > Create |
| Seerr | Settings > General > API Key |
| SABnzbd | Config > General > API Key |
| Jellyfin User IDs | Dashboard > Users > click user > ID is in the URL |

### 3. Configure Environment Variables

Copy the example configuration file and edit it with your API keys and server details:

```bash
cp .env.example .env
nano .env  # or use your preferred editor
```

Fill in your actual values for the API keys and service URLs.

### 4. Build and Run

```bash
docker compose pull && docker compose up -d
```

## Environment Variables

All configuration is done via the `.env` file. See `.env.example` for a template.

| Variable | Default | Description |
|----------|---------|-------------|
| `SONARR_URL` | Required | Sonarr server URL |
| `SONARR_API_KEY` | | Sonarr API key |
| `JELLYFIN_URL` | Required | Jellyfin server URL |
| `JELLYFIN_API_KEY` | | Jellyfin API key |
| `SEERR_URL` | Required | Seerr server URL |
| `SEERR_API_KEY` | | Seerr API key (required to detect which season was requested) |
| `SEERR_USER_ID` | | Seerr user ID to attribute Trakt discovery requests to (see below) |
| `JELLYFIN_USER_IDS` | | Comma-separated Jellyfin user IDs to monitor for watch progress |
| `WATCH_THRESHOLD` | `0.75` | How much of a season must be watched before the next unlocks (0.75 = 75%) |
| `RUN_INTERVAL_MINUTES` | `15` | How often the polling loop runs (in minutes) |
| `NEW_SERIES_LOOKBACK_HOURS` | `48` | How far back to look for newly added series |
| `DRY_RUN` | `false` | Set to `true` to log what would happen without making changes |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `RUN_CATCHUP_ON_START` | `false` | Process all existing series on container start |
| `WEBHOOK_PORT` | `9191` | Port for the webhook listener |
| `SABNZBD_URL` | Required | SABnzbd server URL |
| `SABNZBD_API_KEY` | | SABnzbd API key (required for download priority management) |
| `PLAYBACK_CHECK_INTERVAL` | `45` | How often to check for active playback (in seconds) |
| `TRAKT_CLIENT_ID` | | Trakt API client ID (from https://trakt.tv/oauth/applications) |
| `TRAKT_CLIENT_SECRET` | | Trakt API client secret |
| `TRAKT_DISCOVERY_ENABLED` | `false` | Enable/disable automated Trakt discovery loop |
| `TRAKT_DISCOVERY_TIME` | `00:00` | Time of day to run discovery, in `HH:MM` 24-hour format (local time in `TRAKT_DISCOVERY_TZ`) |
| `TRAKT_DISCOVERY_TZ` | `UTC` | IANA timezone for `TRAKT_DISCOVERY_TIME` (e.g. `America/Chicago`, `America/New_York`, `America/Los_Angeles`) |
| `TRAKT_DISCOVER_SHOWS` | `true` | Discover TV shows |
| `TRAKT_DISCOVER_MOVIES` | `true` | Discover movies |
| `TRAKT_LISTS` | `recommended,watchlist,trending,popular,anticipated` | Which Trakt lists to check (processed in order; personalized lists first ensures they get priority) |
| `TRAKT_MIN_RATING` | `7.0` | Minimum Trakt rating to request |
| `TRAKT_MIN_VOTES` | `100` | Minimum vote count to request |
| `TRAKT_MAX_REQUESTS_PER_CYCLE` | `10` | Max new requests per discovery cycle (split evenly between shows/movies if per-type limits not set) |
| `TRAKT_MAX_SHOW_REQUESTS` | | Max show requests per cycle (overrides even split) |
| `TRAKT_MAX_MOVIE_REQUESTS` | | Max movie requests per cycle (overrides even split) |
| `TRAKT_ITEMS_PER_LIST` | `20` | How many items to fetch per list |
| `TRAKT_LANGUAGES` | `en` | Language filter |
| `TRAKT_GENRES` | | Genre inclusion filter (comma-separated, leave empty for all) |
| `TRAKT_EXCLUDE_GENRES` | | Genre exclusion filter (comma-separated, e.g., `animation,reality,talk-show`) |
| `TRAKT_YEARS` | | Year filter (e.g., `2020-2026`) — applied at both API and application level |
| `FILTER_CONTENT_RATINGS` | | Allow-list for content ratings using Trakt certification — no `TMDB_API_KEY` needed (e.g., `TV-14,TV-MA,PG-13,R`). TV values: `TV-Y`, `TV-Y7`, `TV-G`, `TV-PG`, `TV-14`, `TV-MA`. Movie values: `G`, `PG`, `PG-13`, `R`, `NC-17`. Empty = all ratings allowed. |
| `FILTER_EXCLUDE_CONTENT_RATINGS` | | Deny-list for content ratings — blocks items with these ratings (e.g., `TV-Y,TV-Y7,G,PG`). Runs alongside the allow-list; either can block an item. |
| `TMDB_API_KEY` | | TMDB API key (enables episode count, show status, show type, network, and season count filters — get a free key at https://www.themoviedb.org/settings/api) |
| `TMDB_MAX_EPISODES` | `0` | Skip shows with more than this many total episodes (0 = disabled, requires `TMDB_API_KEY`) |
| `TMDB_ALLOWED_SHOW_STATUS` | | Only allow shows with these statuses (comma-separated, requires `TMDB_API_KEY`). Values: `Returning Series`, `Planned`, `In Production`, `Ended`, `Canceled`, `Pilot`. Empty = all statuses allowed. |
| `TMDB_EXCLUDE_SHOW_TYPES` | | Skip shows of these TMDB types (comma-separated, requires `TMDB_API_KEY`). Values: `Scripted`, `Miniseries`, `Documentary`, `Reality`, `News`, `Talk Show`. Note: values with spaces (e.g. `Talk Show`) are safe in `.env` — the whole line is read as-is. |
| `TMDB_ALLOWED_NETWORKS` | | Only allow shows from these networks (comma-separated, requires `TMDB_API_KEY`). Examples: `HBO`, `Netflix`, `AMC`, `FX`, `Apple TV+`, `Hulu`, `Disney+`, `Showtime`. |
| `TMDB_MAX_SEASONS` | `0` | Skip shows with more than this many seasons (0 = disabled, requires `TMDB_API_KEY`). Complements `TMDB_MAX_EPISODES` — use one or both. |
| `TMDB_ORIGINAL_LANGUAGE` | | Filter by original production language (comma-separated ISO 639-1 codes, requires `TMDB_API_KEY`). More accurate than `TRAKT_LANGUAGES` which uses metadata language. Applies to both shows and movies. Examples: `en`, `en,fr`, `ko,ja`. Empty = all languages allowed. |
| `TRAKT_SEERR_RECHECK_DAYS` | `365` | Days before a `skipped_exists` record expires and the item is re-evaluated. Useful with auto-deletion tools (e.g. Jellysweep) — content deleted from your library will re-enter the discovery pipeline after this window and may be re-requested. `0` = permanent (never re-check). |
| `TRAKT_PREMIUM_BYPASS_ENABLED` | `true` | Allow high-rated content from configured lists to bypass year/status filters |
| `TRAKT_PREMIUM_BYPASS_MIN_RATING` | `8.0` | Minimum rating to qualify for bypass |
| `TRAKT_PREMIUM_BYPASS_LISTS` | `recommended,watchlist` | List sources eligible for bypass |
| `TRAKT_PREMIUM_BYPASS_FILTERS` | `year,status` | Which filters the bypass can override (`year`, `status`, or both) |

### Finding Your Seerr User ID

The `SEERR_USER_ID` setting controls which Seerr user Trakt discovery requests are attributed to. This matters if you use tools like Jellysweep that act on content based on who requested it.

**Option A — Seerr web UI:**
1. Go to **Settings → Users**
2. Click **Edit** on the user you want
3. The numeric ID is in the page URL: `.../settings/users/16/edit` → ID is `16`

**Option B — API:**
```bash
curl -s -H "X-Api-Key: YOUR_SEERR_API_KEY" "http://YOUR_SEERR_URL/api/v1/user" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for u in data.get('results', []):
    print(f'  ID: {u[\"id\"]}, Name: {u.get(\"displayName\", \"?\")}, Email: {u.get(\"email\", \"?\")}')
"
```

Set `SEERR_USER_ID` in your `.env` to the numeric ID of the desired user. If not set, requests are attributed to the API key owner (typically the admin account).

---

## CLI Commands

Run these from your server terminal:

### View Logs
```bash
docker logs -f media-automation
```

### List All Processed Series
```bash
docker exec media-automation python /app/media_automation.py list
```
Shows every series the script has processed, with Sonarr IDs and timestamps.

### Reprocess a Specific Series
```bash
docker exec media-automation python /app/media_automation.py reprocess <sonarr_id>
```
Clears the series from the database and reprocesses it. Use this if monitoring got set wrong or you want to re-run the logic.

**How to find the Sonarr ID:**
1. **From the list command:** Run `docker exec media-automation python /app/media_automation.py list` to see all processed series with their IDs
2. **From Sonarr UI:** Go to the series page in Sonarr and look at the URL - it will be `http://your-sonarr:8989/series/<series-name>`. Click on the series to open the detail page, then check the browser's address bar or network tab for the numeric ID
3. **From Sonarr API:** Visit `http://your-sonarr:8989/api/v3/series?apikey=<your-api-key>` and search for your series in the JSON response - the `id` field is the Sonarr ID

### Manually Trigger a Full Run
```bash
docker exec media-automation python /app/media_automation.py
```
Runs the full polling cycle immediately: checks for new series, checks watch progress, cleans up stale DB entries.

### Process a Single Series (Webhook Mode)
```bash
docker exec media-automation python /app/media_automation.py webhook <sonarr_id>
```
Processes a single series as if it just arrived via webhook. Queries Seerr for the requested season(s) and sets monitoring accordingly.

### Check Active Playback
```bash
docker exec media-automation python /app/media_automation.py playback
```
Checks Jellyfin for active playback sessions and unlocks/prioritizes seasons if a user is watching a preview E01.

### Run Catch-up for Existing Series
```bash
docker exec media-automation python /app/media_automation.py catchup
```
One-time processing for series that were already in Sonarr before this script was installed. Respects seasons that already have downloaded files.

### Dry Run
```bash
docker exec -e DRY_RUN=true media-automation python /app/media_automation.py
```
Logs what changes would be made without actually modifying anything.

### Show Help
```bash
docker exec media-automation python /app/media_automation.py help
```

## Update to Latest Version

```bash
docker compose pull && docker compose up -d
```

## How the Database Works

The script uses a SQLite database at `/data/media_automation.db` (persisted via Docker volume) to track:

- **processed_series**: Which series have already had their monitoring configured
- **unlocked_seasons**: Which seasons have been fully unlocked (either by initial request or by watch progress)
- **priority_boosts**: Which seasons have had their SABnzbd download priorities boosted (prevents re-boosting on every poll cycle)

The database auto-cleans entries for series that no longer exist in Sonarr (e.g., deleted by JellySweep). You never need to manually edit the database under normal operation.

## Troubleshooting

**Show downloaded all seasons instead of just the requested one**
- Check that `SEERR_API_KEY` is set. Without it, the script can't determine which season was requested and falls back to Season 1.
- Check logs for "Seerr: Found request for..." to verify the lookup worked.

**Webhook was skipped**
- Look for "Script already running, skipping" in the logs. This only happens for polling runs, not webhooks. Webhooks have their own queue and will wait for each other.
- The series will still be picked up on the next 15-minute poll cycle.

**Want to change what season is fully monitored**
- Use the `reprocess` command after adjusting the request in Seerr.

**Watch progress not triggering next season**
- Verify the user's Jellyfin ID is in `JELLYFIN_USER_IDS`.
- Check that the watch threshold has been met (default 75%).
- Run `docker exec media-automation python /app/media_automation.py` to trigger a manual check.
