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

### In-progress weekly priority boost

When a new episode arrives for a season someone has been actively watching, it jumps the queue. On each polling cycle the script checks recent Jellyfin play activity: if the most-recent play of a season falls within `INPROGRESS_BOOST_WINDOW_DAYS` (default `7`), any newly grabbed episode of that season is bumped to **High priority** in SABnzbd so the next episode of a show you're mid-watch on doesn't sit behind bulk back-catalog downloads. Set `INPROGRESS_BOOST_WINDOW_DAYS=0` to disable.

### Matched-by-ID auto-import

Sonarr and Radarr sometimes park a perfectly good download in the queue with **"matched by ID — Manual Import required"**, waiting for a human to click Import. When `AUTO_IMPORT_BLOCKED` is `true` (the default), the script detects these blocked items each cycle and triggers the import automatically — Sonarr out of the box, and Radarr when `RADARR_URL`/`RADARR_API_KEY` are configured. Each instance is handled independently so a failure on one never blocks the other.

## Trakt Content Discovery

Automatically discovers trending, popular, and recommended content via the Trakt API and requests it through Seerr. TV shows request only Season 1 — the core automation's E01-preview logic handles progressive unlocking as people watch.

### What it monitors
- **Trending** — Currently most-watched shows and movies
- **Popular** — All-time most popular
- **Anticipated** — Most anticipated upcoming releases
- **Recommended** — Personalized recommendations based on your Trakt watch history
- **Watchlist** — Your Trakt watchlist
- **AI** *(optional)* — Gemini-powered picks matched to your watch history and current cross-platform streaming trends — see [AI-Powered Discovery](#ai-powered-discovery-gemini)

### Filtering pipeline
Each discovered item — including AI nominations — goes through these filters before being requested:
1. **Already discovered** — Skips items seen in previous cycles
2. **Already watched on Trakt** — Skips your complete watch history (import your Netflix/Amazon/etc. history to Trakt for best results)
3. **Rating threshold** — Skips items below `TRAKT_MIN_RATING` (default: 7.0)
4. **Vote threshold** — Skips items with fewer than `TRAKT_MIN_VOTES` (default: 100)
5. **Year filter** — Skips items outside `TRAKT_YEARS` range (backup for API-level filter that some list types ignore)
6. **Genre exclusion** — Skips items matching `TRAKT_EXCLUDE_GENRES` (e.g., `animation,reality,talk-show`)
7. **Content rating** — `FILTER_CONTENT_RATINGS` (allow-list) and `FILTER_EXCLUDE_CONTENT_RATINGS` (deny-list) use Trakt's certification field directly — no `TMDB_API_KEY` required
8. **TMDB filters** (optional, requires `TMDB_API_KEY`) — Episode count (`TMDB_MAX_EPISODES`), show status (`TMDB_ALLOWED_SHOW_STATUS`), show type (`TMDB_EXCLUDE_SHOW_TYPES`), networks (`TMDB_ALLOWED_NETWORKS`, `TMDB_DISALLOWED_NETWORKS`), **streaming providers** (`TMDB_DISALLOWED_PROVIDERS`, shows only — recommended over networks for catching streaming distributors), season count (`TMDB_MAX_SEASONS`), original language (`TMDB_ORIGINAL_LANGUAGE`)
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

### Cross-List Priority Discovery

When enabled (default), items appearing on both a **source list** (e.g. `recommended`) and a **target list** (e.g. `trending`) are processed first — these are the highest-confidence picks because they combine personal recommendation with current popularity.

After cross-list items are requested, remaining download slots are filled from individual lists in the normal `TRAKT_LISTS` order.

| Variable | Default | Description |
|----------|---------|-------------|
| `TRAKT_CROSS_LIST_PRIORITY` | `true` | Enable/disable the two-pass system |
| `TRAKT_CROSS_LIST_SOURCES` | `recommended,watchlist` | Lists providing the "personal signal" |
| `TRAKT_CROSS_LIST_TARGETS` | `trending` | Lists providing the "popularity signal" |

**How it works:** All configured lists are fetched upfront. Items found on both a source and target list are processed first (pass 1), using the source list for premium bypass eligibility. Remaining items are processed in the standard `TRAKT_LISTS` order (pass 2). Set `TRAKT_CROSS_LIST_PRIORITY=false` to revert to the original sequential behaviour.

### AI-Powered Discovery (Gemini)

An optional `ai` pseudo-source for `TRAKT_LISTS` that asks Google Gemini to nominate titles instead of reading a Trakt list. Place it first to give AI picks first claim on the request budget:

```
TRAKT_LISTS=ai,recommended,watchlist,trending,popular,anticipated
```

**How it works:** Once per discovery cycle, the script builds a taste profile from your recent Trakt watch history (titles, years, genres, play counts — `AI_HISTORY_ITEMS` per type), attaches Trakt trending + TMDB trending context and a do-not-suggest list (everything already watched, requested, or in your library), and makes **one** Gemini call covering both shows and movies. With `AI_WEB_SEARCH=true` the model also grounds its picks in live Google Search results — so "what's hot across Netflix/Max/Apple TV/Disney+ this week" is real data, not model memory. Each suggestion is then resolved to a real Trakt/TMDB ID via Trakt search (suggestions that don't resolve are dropped), and the survivors enter the **same filtering pipeline as every other source** — the AI only nominates; your rating, year, genre, network, and dedup filters still decide.

| Variable | Default | Description |
|----------|---------|-------------|
| `GEMINI_API_KEY` | *(empty)* | Free API key from https://aistudio.google.com/apikey — keep it only in your local `.env` |
| `AI_MODEL` | `gemini-flash-latest` | Floating alias that tracks Google's latest stable Flash model — keeps working as Google ships new versions |
| `AI_WEB_SEARCH` | `true` | Ground picks in live Google Search (requires a current-generation model; free tier caps grounded calls per day) |
| `AI_HISTORY_ITEMS` | `50` | Recent watch-history items per type sent as the taste profile |
| `AI_SUGGESTIONS_MULTIPLIER` | `3` | Candidates requested per type, as a multiple of your request limit (more = more likely to fill the budget) |
| `AI_TIMEOUT_SECONDS` | `300` | Max wait for the Gemini response. Keep `TRAKT_SCRIPT_TIMEOUT` ≥ this + 120 (set `TRAKT_SCRIPT_TIMEOUT=600`) or the scheduler kills the run |
| `AI_MIN_RATING` | = `TRAKT_MIN_RATING` | AI-source-only rating floor. Lower it (e.g. `6.5`) to let newer titles through without loosening other lists |
| `AI_MIN_VOTES` | = `TRAKT_MIN_VOTES` | AI-source-only vote floor. Lower it (e.g. `20`) for brand-new titles that haven't accumulated Trakt votes yet |

The AI is also told about your `TMDB_DISALLOWED_NETWORKS` / `TMDB_DISALLOWED_PROVIDERS` so it avoids suggesting titles exclusive to platforms you filter out — without that, picks on blocked platforms (a common cause of "0 requests") waste the budget.

**Setup:** get a key at https://aistudio.google.com/apikey, set `GEMINI_API_KEY` in `.env`, add `ai` to `TRAKT_LISTS`, set `TRAKT_SCRIPT_TIMEOUT=600`, recreate the container. Test with a dry run:

```bash
docker exec -e DRY_RUN=true -e TRAKT_LISTS=ai media-automation python -u /app/trakt_discovery.py discover
```

**Failure behaviour (fail-loud):** if the Gemini call fails for any reason — invalid key, deprecated/renamed model, quota exhausted, network error — the cycle logs an error, fires **one** alert through the configured webhook/email channels (same channels as the Trakt token alerts), and falls through to the remaining `TRAKT_LISTS` sources. A broken AI config can never break discovery, and you'll know about it without checking logs.

**Notes:**
- A grounded call takes 10–30 s. If your cycle already runs near `TRAKT_SCRIPT_TIMEOUT` (default 300 s), raise it.
- `DRY_RUN` skips Seerr requests and DB writes but still makes the real Gemini call (that's the point of the test) — repeated dry runs count against the grounded-call free-tier daily cap.
- Power-user options: add `ai` to `TRAKT_CROSS_LIST_SOURCES` to treat AI nominations as a "personal signal" in two-pass mode, or to `TRAKT_PREMIUM_BYPASS_LISTS` to let high-rated AI picks bypass year/status filters.
- `TRAKT_LANGUAGES` is an API-level list parameter, so it doesn't constrain AI suggestions (the prompt hints at it, but it's soft). If language filtering matters to you, set `TMDB_ORIGINAL_LANGUAGE` — that's a hard filter that applies to AI picks too.

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
- Network names must match TMDB exactly, including spacing and capitalisation — e.g. `Apple TV` not `Apple TV+` (TMDB dropped the `+` in a 2025 rebrand), `The CW` not `CW`.
- If a show has no network data in TMDB (rare), it passes through rather than being blocked.

**`TMDB_DISALLOWED_NETWORKS`**
- Inverse of `TMDB_ALLOWED_NETWORKS` — blocks any show whose TMDB networks include a disallowed name.
- Use this when you want to exclude a few specific networks rather than maintaining a large allow-list.
- Can be used independently of `TMDB_ALLOWED_NETWORKS`. Combining both works — a show must not be on the disallowed list AND must be on the allowed list.
- Same naming rules: network names must match TMDB exactly. Shows with no network data pass through.
- **Known limitation:** checks only the originating broadcast network, not the streaming distributor. A show like *Bodyguard* (BBC One on TMDB, Netflix in reality) slips through. Use `TMDB_DISALLOWED_PROVIDERS` below for the more reliable filter.

**`TMDB_DISALLOWED_PROVIDERS`** *(shows only — recommended)*
- Blocks shows currently streaming on the listed subscription providers in your configured region. Uses TMDB's Watch Providers API, which tracks the actual streaming distributor — so it catches shows the `networks` filter misses (e.g. *Bodyguard* via Netflix) and survives network rebrands.
- Only the `flatrate` (subscription-included) bucket is checked. Rent/buy providers are ignored, so the filter never blocks content you'd buy separately.
- Match is case-sensitive `startswith`, not exact — `Apple TV` catches `Apple TV Amazon Channel`, but `Max` does **not** catch `Cinemax`. Pick the most-specific prefix that catches all the bundle variants for a given service.
- **Movies are unaffected** — theatrical releases often have higher-quality non-streaming versions available (UHD Blu-ray rips), and you probably want those even when the streaming version is on a service you subscribe to. The filter is deliberately TV-only.
- Region is set via `TMDB_PROVIDER_REGION` (ISO 3166-1 country code, default `US`).
- Examples: `Netflix, Apple TV, HBO Max, Max, Disney Plus, Hulu, Amazon Prime Video`.

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
4. Authenticate: `docker exec cascade-media python -u /app/trakt_discovery.py auth`
5. Visit the URL displayed, enter the code
6. Set `TRAKT_DISCOVERY_ENABLED=true` in `.env` and restart

### Commands
```bash
# Authenticate with Trakt
docker exec cascade-media python -u /app/trakt_discovery.py auth

# Check token and discovery stats
docker exec cascade-media python -u /app/trakt_discovery.py status

# Dry run (see what would be requested without making changes)
docker exec -e DRY_RUN=true cascade-media python -u /app/trakt_discovery.py discover

# Run discovery now
docker exec cascade-media python -u /app/trakt_discovery.py discover

# Clear discovered items for a fresh start (keeps request log)
docker exec cascade-media python -u /app/trakt_discovery.py reset

# Re-authenticate (clear tokens and start over)
docker exec cascade-media python -u /app/trakt_discovery.py reauth

# Validate every integration's connection/config (see below)
docker exec cascade-media python -u /app/trakt_discovery.py validate

# Send a real test alert and assert each channel delivered (see below)
docker exec cascade-media python -u /app/trakt_discovery.py test-alert --verify
```

### `validate` — check every integration

`validate` probes each service the stack depends on and prints a `✓`/`✗` line per integration:

```bash
docker exec cascade-media python -u /app/trakt_discovery.py validate
```

It checks **Trakt** (token presence/validity + an authed ping), **TMDB**, **Seerr**, **Sonarr**, **Radarr**, **Jellyfin** (server reachability plus that each `JELLYFIN_USER_IDS` entry is a real UUID, not a display name), **SABnzbd**, and **alert channels**. Required services (Trakt, Sonarr, Jellyfin) failing makes the command exit non-zero; optional services that aren't configured (TMDB, Seerr, Radarr, SABnzbd, alerts) are reported as skipped and don't fail the run. Add `--send` to fire a real test alert as part of the run. Secrets in any echoed URL or error are redacted.

### `test-alert --verify` — confirm alert delivery

```bash
docker exec cascade-media python -u /app/trakt_discovery.py test-alert --verify
```

Sends a test alert and prints a per-channel delivery status (`✓`/`✗` for webhook and email). It exits non-zero if any configured channel failed to deliver, so it's safe to use in a health script.

---

## Setup

### 1. Configure Sonarr Webhook

In Sonarr, go to **Settings > Connect > Add > Webhook**:
- **Name**: Media Automation
- **URL**: `http://<your-server-ip>:9191`
- **Events**: Check "On Series Add"

### 2. Disable Jellyseerr/Overseerr "Enable Automatic Search" on Sonarr

In Jellyseerr (or Overseerr), go to **Settings > Services > Sonarr**, edit each Sonarr server entry (pencil icon), and **uncheck "Enable Automatic Search"**. Save. Repeat for every Sonarr server you have configured (e.g. a 4K-only instance).

**Why:** the cascade only works correctly if cascade-media controls when searches fire. With "Enable Automatic Search" on, Sonarr fires its own `MissingEpisodeSearch` the instant Seerr creates a multi-season series and grabs every episode of every season before cascade-media's monitoring can take effect. With it off, Seerr tells Sonarr to skip that search, and cascade-media's explicit `SeasonSearch` (target season) and `EpisodeSearch` (preview E01s) at ~T+17 seconds are the only searches that run.

**Cost:** ~15 seconds of extra delay between Seerr "Request" and the first NZB grab. Movies (Radarr) are unaffected.

Leave Sonarr's other Seerr settings (Default Server, Quality Profile, Root Folder, Enable Scan, etc.) unchanged.

### 3. Get API Keys

| Service | Where to find it |
|---------|-----------------|
| Sonarr | Settings > General > API Key |
| Jellyfin | Dashboard > API Keys > Create |
| Seerr | Settings > General > API Key |
| SABnzbd | Config > General > API Key |
| Jellyfin User IDs | Dashboard > Users > click user > ID is in the URL |

### 4. Configure Environment Variables

Copy the example configuration file and edit it with your API keys and server details:

```bash
cp .env.example .env
nano .env  # or use your preferred editor
```

Fill in your actual values for the API keys and service URLs.

### 5. Build and Run

```bash
docker compose pull && docker compose up -d
```

## Environment Variables

All configuration is done via the `.env` file. See `.env.example` for a template.

| Variable | Default | Description |
|----------|---------|-------------|
| `SONARR_URL` | Required | Sonarr server URL |
| `SONARR_API_KEY` | | Sonarr API key |
| `RADARR_URL` | | Radarr server URL (enables movie auto-import) |
| `RADARR_API_KEY` | | Radarr API key (enables movie auto-import) |
| `AUTO_IMPORT_BLOCKED` | `true` | Auto-resolve Sonarr/Radarr "matched by ID — Manual Import required" queue blocks (Radarr handling needs `RADARR_URL`/`RADARR_API_KEY`) — see [Matched-by-ID auto-import](#matched-by-id-auto-import) |
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
| `INPROGRESS_BOOST_WINDOW_DAYS` | `7` | Weekly priority boost window. A new episode of a season you watched within this many days is bumped to High in SABnzbd. `0` = disabled. See [In-progress weekly priority boost](#in-progress-weekly-priority-boost) |
| `PLAYBACK_CHECK_INTERVAL` | `45` | How often to check for active playback (in seconds) |
| `TRAKT_CLIENT_ID` | | Trakt API client ID (from https://trakt.tv/oauth/applications) |
| `TRAKT_CLIENT_SECRET` | | Trakt API client secret |
| `TRAKT_DISCOVERY_ENABLED` | `false` | Enable/disable automated Trakt discovery loop |
| `TRAKT_DISCOVERY_TIME` | `00:00` | Time of day to run discovery, in `HH:MM` 24-hour format (local time in `TRAKT_DISCOVERY_TZ`) |
| `TRAKT_DISCOVERY_TZ` | `UTC` | IANA timezone for `TRAKT_DISCOVERY_TIME` (e.g. `America/Chicago`, `America/New_York`, `America/Los_Angeles`) |
| `TRAKT_DISCOVER_SHOWS` | `true` | Discover TV shows |
| `TRAKT_DISCOVER_MOVIES` | `true` | Discover movies |
| `TRAKT_LISTS` | `recommended,watchlist,trending,popular,anticipated` | Which Trakt lists to check (processed in order; personalized lists first ensures they get priority). Also accepts `ai` — see [AI-Powered Discovery](#ai-powered-discovery-gemini) |
| `TRAKT_MIN_RATING` | `7.0` | Minimum Trakt rating to request |
| `TRAKT_MIN_VOTES` | `100` | Minimum vote count to request |
| `TRAKT_MAX_REQUESTS_PER_CYCLE` | `10` | Max new requests per discovery cycle (split evenly between shows/movies if per-type limits not set) |
| `TRAKT_MAX_SHOW_REQUESTS` | | Max show requests per cycle (overrides even split) |
| `TRAKT_MAX_MOVIE_REQUESTS` | | Max movie requests per cycle (overrides even split) |
| `TRAKT_ITEMS_PER_LIST` | `20` | How many items to fetch per list |
| `TRAKT_CROSS_LIST_PRIORITY` | `true` | Process items appearing on both a source and target list first (highest-confidence picks) |
| `TRAKT_CROSS_LIST_SOURCES` | `recommended,watchlist` | Lists providing the "personal signal" for cross-list priority |
| `TRAKT_CROSS_LIST_TARGETS` | `trending` | Lists providing the "popularity signal" for cross-list priority |
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
| `TMDB_ALLOWED_NETWORKS` | | Only allow shows from these networks (comma-separated, requires `TMDB_API_KEY`). Examples: `HBO`, `Netflix`, `AMC`, `FX`, `Apple TV`, `Hulu`, `Disney+`, `Showtime`. |
| `TMDB_DISALLOWED_NETWORKS` | | Skip shows from these networks (comma-separated, requires `TMDB_API_KEY`). Inverse of `TMDB_ALLOWED_NETWORKS`. Examples: `Netflix`, `Hulu`, `Disney+`. Note: only checks originating network — for streaming distributors use `TMDB_DISALLOWED_PROVIDERS`. |
| `TMDB_DISALLOWED_PROVIDERS` | | Skip **shows only** that are currently streaming on these subscription providers in `TMDB_PROVIDER_REGION` (comma-separated, requires `TMDB_API_KEY`). Uses TMDB Watch Providers API — more reliable than `TMDB_DISALLOWED_NETWORKS` for catching streaming distributors and surviving network rebrands. Case-sensitive `startswith` match (so `Apple TV` catches `Apple TV Amazon Channel` but `Max` does not catch `Cinemax`). Movies are unaffected. Examples: `Netflix, Apple TV, HBO Max, Max`. |
| `TMDB_PROVIDER_REGION` | `US` | ISO 3166-1 country code for the Watch Providers lookup (used by `TMDB_DISALLOWED_PROVIDERS`). |
| `TMDB_MAX_SEASONS` | `0` | Skip shows with more than this many seasons (0 = disabled, requires `TMDB_API_KEY`). Complements `TMDB_MAX_EPISODES` — use one or both. |
| `TMDB_ORIGINAL_LANGUAGE` | | Filter by original production language (comma-separated ISO 639-1 codes, requires `TMDB_API_KEY`). More accurate than `TRAKT_LANGUAGES` which uses metadata language. Applies to both shows and movies. Examples: `en`, `en,fr`, `ko,ja`. Empty = all languages allowed. |
| `TRAKT_SEERR_RECHECK_DAYS` | `365` | Days before a `skipped_exists` record expires and the item is re-evaluated. Useful with auto-deletion tools (e.g. Jellysweep) — content deleted from your library will re-enter the discovery pipeline after this window and may be re-requested. `0` = permanent (never re-check). |
| `TRAKT_PREMIUM_BYPASS_ENABLED` | `true` | Allow high-rated content from configured lists to bypass year/status filters |
| `TRAKT_PREMIUM_BYPASS_MIN_RATING` | `8.0` | Minimum rating to qualify for bypass |
| `TRAKT_PREMIUM_BYPASS_LISTS` | `recommended,watchlist` | List sources eligible for bypass |
| `TRAKT_PREMIUM_BYPASS_FILTERS` | `year,status` | Which filters the bypass can override (`year`, `status`, or both) |
| `GEMINI_API_KEY` | | Google Gemini API key — enables the `ai` discovery source (free key at https://aistudio.google.com/apikey) |
| `AI_MODEL` | `gemini-flash-latest` | Gemini model for AI discovery. The default floating alias tracks Google's latest stable Flash model. |
| `AI_WEB_SEARCH` | `true` | Ground AI picks in live Google Search results (current-generation models only; free tier caps grounded calls per day) |
| `AI_HISTORY_ITEMS` | `50` | Recent watch-history items per type (shows/movies) sent to the AI as the taste profile |
| `AI_SUGGESTIONS_MULTIPLIER` | `3` | Candidates the AI returns per type, as a multiple of the per-type request limit |
| `AI_TIMEOUT_SECONDS` | `300` | Max wait for the Gemini call. Pair with `TRAKT_SCRIPT_TIMEOUT` ≥ this + 120 |
| `AI_MIN_RATING` | `TRAKT_MIN_RATING` | AI-source-only rating floor (lower to admit newer titles) |
| `AI_MIN_VOTES` | `TRAKT_MIN_VOTES` | AI-source-only vote floor (lower for brand-new titles with few Trakt votes) |

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
docker logs -f cascade-media
```

### List All Processed Series
```bash
docker exec cascade-media python /app/media_automation.py list
```
Shows every series the script has processed, with Sonarr IDs and timestamps.

### Reprocess a Specific Series
```bash
docker exec cascade-media python /app/media_automation.py reprocess <sonarr_id>
```
Clears the series from the database and reprocesses it. Use this if monitoring got set wrong or you want to re-run the logic.

**How to find the Sonarr ID:**
1. **From the list command:** Run `docker exec cascade-media python /app/media_automation.py list` to see all processed series with their IDs
2. **From Sonarr UI:** Go to the series page in Sonarr and look at the URL - it will be `http://your-sonarr:8989/series/<series-name>`. Click on the series to open the detail page, then check the browser's address bar or network tab for the numeric ID
3. **From Sonarr API:** Visit `http://your-sonarr:8989/api/v3/series?apikey=<your-api-key>` and search for your series in the JSON response - the `id` field is the Sonarr ID

### Manually Trigger a Full Run
```bash
docker exec cascade-media python /app/media_automation.py
```
Runs the full polling cycle immediately: checks for new series, checks watch progress, cleans up stale DB entries.

### Process a Single Series (Webhook Mode)
```bash
docker exec cascade-media python /app/media_automation.py webhook <sonarr_id>
```
Processes a single series as if it just arrived via webhook. Queries Seerr for the requested season(s) and sets monitoring accordingly.

### Check Active Playback
```bash
docker exec cascade-media python /app/media_automation.py playback
```
Checks Jellyfin for active playback sessions and unlocks/prioritizes seasons if a user is watching a preview E01.

### Run Catch-up for Existing Series
```bash
docker exec cascade-media python /app/media_automation.py catchup
```
One-time processing for series that were already in Sonarr before this script was installed. Respects seasons that already have downloaded files.

### Dry Run
```bash
docker exec -e DRY_RUN=true cascade-media python /app/media_automation.py
```
Logs what changes would be made without actually modifying anything.

### Show Help
```bash
docker exec cascade-media python /app/media_automation.py help
```

## Update to Latest Version

The easiest way to update is the bundled helper script, which pulls the latest code **and** image, recreates the container, and cleans up the old image:

```bash
./update.sh
```

**Why not just `docker compose pull`?** `docker compose pull` updates only the container *image*. It does **not** touch the on-disk files in your checkout — `.env.example`, this `README.md`, the `docker-compose.yml`, and `CHANGELOG.md` all live in git, not the image. To get new env-var defaults, docs, and compose changes you need a `git pull` as well. `update.sh` does both:

```bash
#!/bin/bash
set -e
cd /mnt/user/appdata/media-automation
git pull
docker compose pull
docker compose up -d --force-recreate
docker image prune -f
```

If you prefer to do it by hand (image only, no on-disk file updates):

```bash
docker compose pull && docker compose up -d
```

Verify the new image is healthy:

```bash
docker compose ps                            # wait for STATUS to show (healthy) — 30-60s
docker logs cascade-media --tail 20       # confirm no startup errors
```

To pin to a specific version instead of `:latest`, edit the `image:` line in `docker-compose.yml`:

```yaml
image: ghcr.io/normalee1993/cascade-media:v1.2.0
```

Released versions and their changes are documented in [CHANGELOG.md](./CHANGELOG.md).

## Container Operations

The container is designed to run unattended. Several operator-facing behaviors were added in v1.2.0 — none require configuration, but knowing they exist helps when something goes wrong.

### Health checks
A built-in Docker `HEALTHCHECK` probes the webhook HTTP server on port 9191 every 30 seconds. If the scheduler or its webhook listener wedges, the container is marked unhealthy and surfaces in `docker compose ps` (and Unraid's container view).

```bash
docker compose ps                                                   # column "STATUS" shows (healthy)
docker inspect cascade-media --format '{{.State.Health.Status}}' # → "healthy"
docker inspect cascade-media --format '{{json .State.Health}}'   # full probe history
```

### Non-root user
The container runs as **UID 99 GID 100** — Unraid's `nobody:users` convention. On Unraid this matches `/mnt/user/appdata/*` ownership so the bind mount works without any host-side `chown`. On other Linux hosts you may need to make the data directory writable by UID 99:

```bash
chown -R 99:100 /path/to/your/data/dir
```

### Log rotation
Docker's `json-file` driver is configured in `docker-compose.yml` to cap logs at 10MB per file × 5 files (~50MB total). Rotation is automatic — `docker logs cascade-media` always works regardless of how long the container has been running.

### Graceful shutdown
The scheduler handles SIGTERM and SIGINT cleanly. `docker stop cascade-media` typically exits in ~1 second: the background loops wake from their sleeps, the webhook HTTP server is shut down, and the worker pool cancels pending tasks. `docker-compose.yml` sets `stop_grace_period: 30s` to give the scheduler room to drain in-flight subprocess work before Docker escalates to SIGKILL.

## How the Database Works

The script uses a SQLite database at `/data/media_automation.db` (persisted via Docker volume) to track:

- **processed_series**: Which series have already had their monitoring configured
- **unlocked_seasons**: Which seasons have been fully unlocked (either by initial request or by watch progress)
- **priority_boosts**: Which seasons have had their SABnzbd download priorities boosted (prevents re-boosting on every poll cycle)

The database auto-cleans entries for series that no longer exist in Sonarr (e.g., deleted by JellySweep). You never need to manually edit the database under normal operation.

### Where the data lives (and why renaming the container is safe)

The SQLite database **and** the Trakt OAuth token live on the **host** data folder — the path you bind-mount to `/data` — not inside the container. In `docker-compose.yml` that mapping is `${DATA_DIR:-./data}:/data`; on Unraid the template default is `/mnt/user/appdata/cascade-media`. Because all persistent state is on the host, the container itself is disposable:

- **Renaming the container is non-destructive.** The container was historically named `media-automation` and is now standardized on `cascade-media`. Renaming does **not** touch or orphan your data — just keep the **same** `/data` mapping when you recreate. The safe sequence is **stop → rm → recreate** with the identical volume mapping:

  ```bash
  docker stop cascade-media
  docker rm cascade-media
  docker compose up -d        # recreates as cascade-media, same ${DATA_DIR}:/data mapping
  ```

  Your DB and Trakt token persist across the rename — no re-authentication needed. (If you previously set `DATA_DIR`, keep it pointed at the same host folder.)

- **Keep the data dir on a cache-backed path, not the FUSE share.** Point `DATA_DIR` at a pool/cache-backed location (e.g. `/mnt/cache/appdata/cascade-media`) rather than the Unraid FUSE user share (`/mnt/user/...`). SQLite uses WAL mode, and WAL's memory-mapped locking can be flaky on `shfs`/FUSE, which can surface as intermittent "database is locked" or readonly errors — exactly the kind of write failure that can lose a rotated Trakt token. A cache/pool path avoids the FUSE layer entirely.

## Troubleshooting

**Show downloaded all seasons instead of just the requested one**
- Check that `SEERR_API_KEY` is set. Without it, the script can't determine which season was requested and falls back to Season 1.
- Check logs for "Seerr: Found request for..." to verify the lookup worked.

**Webhook was skipped**
- Look for "Script already running, skipping" in the logs. This only happens for polling runs, not webhooks. Webhooks have their own queue and will wait for each other.
- The series will still be picked up on the next 15-minute poll cycle.

**Download stuck in the Sonarr/Radarr queue ("Manual Import required")**
- Symptom: a completed download sits in Activity → Queue as `importBlocked`/`importPending` with *"Found matching series/movie via grab history, but release was matched to series/movie by ID. Automatic import is not possible."*
- Cause: the release name doesn't parse to the title in your library (e.g. `Battlestar.Galactica.2005.S02E01...` is stamped with the season's air-year and doesn't map to library entry `Battlestar Galactica (2003)`), so the *arr can only match by grab-history ID and refuses to auto-import as a safety measure. There is no "force import by ID" toggle in stable Sonarr/Radarr.
- Manual fix: Activity → Queue → click the ⚠️ on the item → **Manual Import** → tick the file (series/episode are pre-filled) → **Import**.
- Stop it re-grabbing the same offender: add a Release Profile *Must Not Contain* rule or a Custom Format that scores down the mis-named release group / wrong-year releases.
- Automating this queue clean-up (scan for the "matched by ID" message → fire the manual import) is planned; not yet implemented.

**Want to change what season is fully monitored**
- Use the `reprocess` command after adjusting the request in Seerr.

**Watch progress not triggering next season**
- Verify the user's Jellyfin ID is in `JELLYFIN_USER_IDS`.
- Check that the watch threshold has been met (default 75%).
- Run `docker exec cascade-media python /app/media_automation.py` to trigger a manual check.

**Got a "Trakt token refresh FAILED" or "Database readonly" alert email**
- Check DB ownership: `stat /mnt/user/appdata/cascade-media/data/media_automation.db` — should be `uid=99 gid=100 mode=664`. If the container can't write to the DB, `save_tokens()` fails *after* Trakt has already rotated the refresh_token, locking you out until manual re-auth.
- Fix permissions from the host: `docker exec --user root cascade-media chown 99:users /data/media_automation.db && docker exec --user root cascade-media chmod 664 /data/media_automation.db`
- Re-authenticate (interactive, ~1 min): `docker exec cascade-media python -u /app/trakt_discovery.py auth`. Visit the URL it prints (`https://trakt.tv/activate`), enter the 8-character code, approve. The script saves the new tokens automatically.
- Confirm with `docker exec -e DRY_RUN=true cascade-media python -u /app/trakt_discovery.py discover` — should show "Loaded N watched shows / N watched movies" with no readonly errors.
**Container reports unhealthy** *(v1.2.0+)*
- Read the probe history: `docker inspect cascade-media --format '{{json .State.Health}}'`. Repeated non-zero `ExitCode` entries mean the webhook server is not responding on port 9191.
- Verify port 9191 isn't being claimed by another process on the host.
- Check container logs: `docker logs cascade-media --tail 50`. A wedged Trakt discovery loop or stalled subprocess will usually show up here.
- Recover by restarting: `docker compose restart cascade-media`.

**Permission denied errors on /data after upgrading to v1.2.0+**
- The container now runs as UID 99 GID 100 (non-root). On Unraid this matches `/mnt/user/appdata` ownership by default — no action needed.
- On other Linux hosts where the data directory is owned by a different UID, fix with `chown -R 99:100 /path/to/your/data/dir` then `docker compose restart`.
