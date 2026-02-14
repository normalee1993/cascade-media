# Media Automation for Unraid

Automatically manages TV show downloads by connecting Jellyseerr, Sonarr, Jellyfin, and SABnzbd. Instead of downloading entire series when someone makes a request, it downloads only what's needed and progressively unlocks more as people watch. When playback is detected or watch thresholds are met, downloads are automatically prioritized in SABnzbd so the next episodes are ready in time.

## How It Works

### When someone requests a show in Jellyseerr

1. Jellyseerr tells Sonarr to add the series (Sonarr monitors ALL episodes by default)
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
| Jellyseerr | Settings > General > API Key |
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
docker compose build && docker compose up -d
```

## Environment Variables

All configuration is done via the `.env` file. See `.env.example` for a template.

| Variable | Default | Description |
|----------|---------|-------------|
| `SONARR_URL` | Required | Sonarr server URL |
| `SONARR_API_KEY` | | Sonarr API key |
| `JELLYFIN_URL` | Required | Jellyfin server URL |
| `JELLYFIN_API_KEY` | | Jellyfin API key |
| `JELLYSEERR_URL` | Required | Jellyseerr server URL |
| `JELLYSEERR_API_KEY` | | Jellyseerr API key (required to detect which season was requested) |
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
Clears the series from the database and reprocesses it. Use this if monitoring got set wrong or you want to re-run the logic. Find the Sonarr ID from the `list` command or from the Sonarr URL (e.g., `http://sonarr:8989/series/supernatural` -> check the series detail page).

### Manually Trigger a Full Run
```bash
docker exec media-automation python /app/media_automation.py
```
Runs the full polling cycle immediately: checks for new series, checks watch progress, cleans up stale DB entries.

### Process a Single Series (Webhook Mode)
```bash
docker exec media-automation python /app/media_automation.py webhook <sonarr_id>
```
Processes a single series as if it just arrived via webhook. Queries Jellyseerr for the requested season(s) and sets monitoring accordingly.

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

## Rebuild After Code Changes

```bash
docker compose build && docker compose up -d
```

## How the Database Works

The script uses a SQLite database at `/data/media_automation.db` (persisted via Docker volume) to track:

- **processed_series**: Which series have already had their monitoring configured
- **unlocked_seasons**: Which seasons have been fully unlocked (either by initial request or by watch progress)
- **priority_boosts**: Which seasons have had their SABnzbd download priorities boosted (prevents re-boosting on every poll cycle)

The database auto-cleans entries for series that no longer exist in Sonarr (e.g., deleted by JellySweep). You never need to manually edit the database under normal operation.

## Troubleshooting

**Show downloaded all seasons instead of just the requested one**
- Check that `JELLYSEERR_API_KEY` is set. Without it, the script can't determine which season was requested and falls back to Season 1.
- Check logs for "Jellyseerr: Found request for..." to verify the lookup worked.

**Webhook was skipped**
- Look for "Script already running, skipping" in the logs. This only happens for polling runs, not webhooks. Webhooks have their own queue and will wait for each other.
- The series will still be picked up on the next 15-minute poll cycle.

**Want to change what season is fully monitored**
- Use the `reprocess` command after adjusting the request in Jellyseerr.

**Watch progress not triggering next season**
- Verify the user's Jellyfin ID is in `JELLYFIN_USER_IDS`.
- Check that the watch threshold has been met (default 75%).
- Run `docker exec media-automation python /app/media_automation.py` to trigger a manual check.
