# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.2.1] - 2026-05-17

### Added
- First automated test suite: 8 stdlib `unittest` regression tests covering production incidents from 2026-05
  - Apple TV+ → Apple TV rebrand (TMDB_DISALLOWED_NETWORKS)
  - HBO listed separately from Max on TMDB
  - TMDB Watch Providers filter + shows-only carve-out
  - Watched-history safety net (PR #1, 2026-05-12)
  - Missing-tokens alert path (PR #3, 2026-05-16)
- CI `test` job in `.github/workflows/docker-build.yml`, gated on both `push` and `pull_request` events
- `build-and-push` job now `needs: test`, so failing tests block GHCR image publish

### Notes
The `:v1.2.1` image is functionally identical to `:v1.2.0` — only `tests/` and `.github/` changed, neither is `COPY`'d into the image. No production redeploy needed after upgrading from v1.2.0.

## [v1.2.0] - 2026-05-17

Major operator-facing improvements: graceful container lifecycle, runtime hardening, and disk-usage safeguards. Closes 5 of 8 actionable items in the technical-debt backlog.

### Added
- **SIGTERM / SIGINT handlers** in `scheduler.py` — `docker stop` now exits cleanly in ~1 second. Background loops use `threading.Event.wait()` instead of `time.sleep()` so they wake immediately on signal. Webhook HTTPServer is shut down explicitly; worker pool drains with `cancel_futures=True`.
- **Docker `HEALTHCHECK`** — probes the webhook GET endpoint on port 9191 every 30 seconds via stdlib `urllib.request` (no `curl` dependency). Container reports `(healthy)` in `docker compose ps`.
- **Non-root container user** — runs as UID 99 GID 100 (Unraid `nobody:users`). Numeric form, no `useradd` needed. Matches `/mnt/user/appdata` ownership so the Unraid bind mount works without any host `chown`.
- **Log rotation** in `docker-compose.yml` — `json-file` driver with `max-size: 10m` and `max-file: 5` caps logs at ~50MB total.
- **`stop_grace_period: 30s`** in `docker-compose.yml` — gives the SIGTERM handler room to drain in-flight subprocess work before Docker escalates to SIGKILL.
- **`SCRIPT_TIMEOUT_MINUTES` and `TRAKT_SCRIPT_TIMEOUT`** documented in `.env.example`. Both were already read by `scheduler.py` but undocumented.

### Removed
- Orphaned `pending_series = deque()` and `pending_lock = threading.Lock()` in `scheduler.py` — dead code from an earlier queueing design that `webhook_executor.submit()` replaced.
- Unused `TRAKT_DISCOVERY_INTERVAL_HOURS` env var — read in `trakt_discovery.py` at import but never referenced. The scheduler uses `TRAKT_DISCOVERY_TIME` for daily clock-time scheduling instead.

### Upgrade notes
- Bind-mount data directory must be writable by UID 99 GID 100. On Unraid this is automatic. On other hosts: `chown -R 99:100 /path/to/data/dir` before recreating the container.
- No environment-variable changes required.

## [v1.1.0] - 2026-05-13

First post-v1.0.0 release. Bundles all interim feature and fix work since the initial release.

### Added
- **Cross-list priority discovery** — items appearing on both a source list (e.g. `recommended`) and a target list (`trending`) are processed first, before filling remaining slots. Configurable via `TRAKT_CROSS_LIST_*` env vars.
- **`TMDB_DISALLOWED_NETWORKS`** filter — skip shows whose originating TMDB network matches a deny list (e.g. `Netflix, Apple TV, Max, HBO`).
- **`TMDB_DISALLOWED_PROVIDERS`** filter — skip shows currently distributed by a streaming provider via the TMDB Watch Providers API. Catches the "Bodyguard" class of bugs (originating network ≠ streaming distributor) and survives network-name rebrands. Case-sensitive `startswith` match. Shows-only by design (movies are unaffected so theatrical releases remain downloadable).
- **Watched-history safety net** — `fetch_watched_ids()` returns `None` per media type when the Trakt fetch fails (auth break, 5xx, network outage). `process_discovered_item()` treats `None` as a hard block, preventing watched items from being re-requested even if the auth path silently breaks.
- **Email alert channel** via SMTP — fires on Trakt token refresh failure. Independent of the existing webhook channel (`ALERT_WEBHOOK_URL`). Gmail-friendly with App Password. Configurable subject prefix.
- **`test-alert` CLI command** — `docker exec media-automation python /app/trakt_discovery.py test-alert` fans a test message through every configured alert channel.
- **`.dockerignore`** — keeps build context lean.
- **Pinned pip dependencies** in the Dockerfile: `requests==2.34.0`, `tzdata==2026.2`.

### Fixed
- **Apple TV+ → Apple TV TMDB rebrand** — exact-string network filter now matches the rebranded value. Documentation in `.env.example` and `SYSTEM_DOCUMENTATION.md` updated accordingly.
- **HBO and Max are separate networks on TMDB** — using only `Max` in the deny list leaked HBO Originals. Documentation updated to highlight both should be listed if the user wants both blocked.
- **Cross-list dedup** — items appearing on multiple Trakt lists are no longer processed multiple times within a single discovery cycle.

## [v1.0.0] - 2026-02-28

Initial public release.

### Added
- **Trakt content discovery** — finds new movies and TV shows from Trakt lists (`recommended`, `watchlist`, `trending`, `popular`, `anticipated`), passes them through a 13-stage filter pipeline, and requests via Seerr.
- **TV cascade logic** in `media_automation.py` — downloads Season 1 fully + Episode 1 previews of every other season. Auto-unlocks the next season when the user crosses a watch threshold (~75%); instant unlock on playback detection.
- **Scheduler** in `scheduler.py` — orchestrates 15-minute polling, 45-second playback checks, and daily Trakt discovery. Sonarr webhook listener on port 9191 for SeriesAdd events.
- **Premium content bypass** — content rated ≥ 8.0 on `recommended`/`watchlist` can skip year/status filters. Does not bypass network/provider filters.
- **OAuth device-code flow** for Trakt authentication.
- **SABnzbd priority boosting** for cascade-related downloads.
- **Docker image** published to GHCR (`ghcr.io/normalee1993/cascade-media`).
- **Unraid Community Applications template** in `templates/cascade-media.xml`.

[v1.2.1]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.1
[v1.2.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.0
[v1.1.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.1.0
[v1.0.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.0.0
