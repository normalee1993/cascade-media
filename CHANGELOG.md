# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.10.0] - 2026-07-14

Statistical taste profile for AI discovery: a locally computed, recency-weighted distillation of watch behavior added to the Gemini prompt alongside (not replacing) the raw history — the combination outperforms either alone per LLM-recommender research. Off by default (`AI_PROFILE=true` to enable).

### Added
- **`taste_profile.py`** (new module) — pure-stdlib statistics over three local sources:
  - **Trakt watched history**: recency-decayed genre shares (`w = 0.5^(days/half_life)`, default 90-day half-life), release-era preference, movie rewatches.
  - **TMDB keywords + credits**, cached incrementally in a new `ai_title_metadata` table (≤40 lookups/cycle; profile matures over ~1 week, steady state ~0 calls): recurring themes, repeatedly watched actors/creators, weighted-median runtimes.
  - **Jellyfin play state for ONE selected user** (`AI_PROFILE_JELLYFIN_USER`, display name or GUID): binged series (≥90% watched within 7 days — strong likes), abandoned series (≤40% watched, idle 60+ days — explicit "avoid similar"), favorites, rewatch counts. Per-person by design: blending five household accounts would smear the pace signals into noise.
  - **Trakt personal ratings** (only if ≥10 exist): 9-10 titles as loves, ≤5 as explicit dislikes.
- The rendered block is persisted to `ai_taste_profile.txt` next to the database — read it to see exactly what is sent to Gemini. All raw play data stays local; only the distilled text leaves the box.
- With a profile present, the raw history section trims to ≤25 rows per type (the profile carries the long-tail signal).
- New env: `AI_PROFILE` (default false), `AI_PROFILE_JELLYFIN_USER`, `AI_PROFILE_HALF_LIFE_DAYS` (90). Documented in `.env.example`.

### Fixed (during verification dry-runs)
- Rewatch ranking crashed on titles without a year (`int < None` tuple comparison).
- Trakt's hyphenated genre slugs (`science-fiction`) never matched the majors list, wrongly declaring sci-fi "never watched" for a household that binged *Dark Matter*.

### Notes
- SemVer: MINOR — inert unless `AI_PROFILE=true`; profile failure of any kind degrades to the profile-less prompt with a warning.
- Verified via isolated DRY_RUN: profile computed from 200 titles + 120 cached TMDB entries + AzureAperture's Jellyfin state; all 4 suggestion slots filled; suggestions visibly shifted toward profile themes (Longlegs, Strange Darling vs. the profile-less run's picks).
- 236 unit tests pass (27 new).

## [v1.9.0] - 2026-07-14

AI discovery efficiency: the model now knows what you actually own, and every prompt is auditable. Fixes the ~21% of AI suggestion slots that were being wasted re-suggesting owned/watched titles (measured over the source's first month live).

### Added
- **Authoritative AI exclusion list.** `collect_ai_exclusions()` merges three sources, deduped by normalized title + type: the full **Sonarr/Radarr catalogs** (ground truth for owned/monitored — new `fetch_library_titles()`), the full **Trakt watched history** (reusing the same per-cycle fetch as the taste-profile summary, cached in `_ai_cache`), and the legacy reactive list from `trakt_discovered` (the only source that remembers titles since deleted from the arrs). Previously the prompt's "DO NOT suggest" block came only from the reactive list — a title had to be surfaced and skipped once before the AI learned it was owned, and its `LIMIT 300` had begun truncating (426 eligible). Any source failing degrades the list with a warning instead of failing the AI cycle.
- **AI prompt/response artifacts.** The exact prompt sent to Gemini and its verbatim reply are written to `ai_prompt_last.txt` / `ai_response_last.txt` next to the database, overwritten each cycle — so audits can read exactly what the model was given instead of reconstructing it from discovery records. Write failures are non-fatal.

### Changed
- `fetch_ai_exclusions()` is now a fallback input to the merged list; its cap raised 300 → 1000.
- Exclusion entries include the year when known (`Title (2024, movie)`) to disambiguate remakes — unless the title already embeds a year (Sonarr disambiguation like `Foundation (2021)`), which is no longer double-printed.
- Sonarr/Radarr env vars (already present for `validate`) are now also used by discovery for the exclusion catalog.

### Notes
- SemVer: MINOR — backward-compatible; no new env vars required.
- Verified via isolated DRY_RUN: exclusion list 975 titles ("library ok"), prompt contains known-owned spot-checks, all 4 suggestion slots filled with fresh titles, zero owned/watched re-suggestions.
- 209 unit tests pass (11 new).

## [v1.8.1] - 2026-06-18

Cascade unlock correctness fixes (caught in production logs) plus validator/docs/deploy finishing touches.

### Fixed
- **Watching a single preview episode no longer unlocks the *next* season.** `check_watch_progress` computed a season's watched % using the number of episodes Jellyfin could see — i.e. only the **downloaded** ones. For a preview-only season that's just the E01 preview, so watching it read as `1/1 = 100%` and tripped the 75% next-season unlock. (Live example: *For All Mankind* S04's preview was watched and S05 was wrongly unlocked + grabbed.) The denominator is now the count of **aired** episodes for that season from Sonarr (`airDateUtc <= now`), so the same watch reads as `1/10 = 10%` and the next season stays locked. If Sonarr's episode list is unavailable, unlocks are skipped for that series this cycle rather than falling back to the buggy count.
- **Watching a preview E01 now unlocks the *rest of that season* via watch history, not just live playback.** The documented "play a preview E01 → unlock that whole season" behavior previously only fired from the active-playback path (a currently-playing session). If the play had already finished (or wasn't caught live), the season's remaining episodes never downloaded. The watch-progress path now also unlocks a still-locked season when its E01 is marked watched.

### Added
- **Radarr in the `validate` command** — `validate` now probes Radarr (`/api/v3/system/status`) alongside the other services. Optional: skipped when `RADARR_URL`/`RADARR_API_KEY` are blank.
- **`update.sh`** deploy helper (repo root) — `git pull` + `docker compose pull` + `up -d --force-recreate` + image prune in one command, so on-disk files (`.env.example`, README, docs) stay in sync with the image. `docker compose pull` alone only updates the image.
- **README** now documents `validate`, the in-progress weekly boost (`INPROGRESS_BOOST_WINDOW_DAYS`), matched-by-ID auto-import (`RADARR_*`, `AUTO_IMPORT_BLOCKED`), `test-alert --verify`, and an "Update to Latest Version" section.

### Changed (internal)
- The three season-unlock sites (next-season, current-season, active-playback) now share one `unlock_and_download_season()` helper — identical monitor → SeasonSearch → mark-unlocked → boost behavior, including the interruptible shutdown wait.

### Notes
- SemVer: PATCH — primarily bug fixes; the Radarr probe / `update.sh` / README additions are small and backward-compatible.
- 198 unit tests pass (15 new).

## [v1.8.0] - 2026-06-18

Tier 2 backlog: operational auto-recovery and alert verification.

### Added
- **"Matched by ID" auto-import (Sonarr + Radarr).** Completed downloads whose release name doesn't self-parse to a library title (e.g. `Battlestar.Galactica.2005.S02E01`, or movies like `Batman: The Long Halloween (2021)`) get stuck as `importBlocked`/`importPending` — *"matched … by ID. Automatic import is not possible / Manual Import required."* The scheduler now detects these (state is import-blocked **and** the message matches the by-ID signature) and resolves them via `GET /manualimport?downloadId=…` → `POST /command {"name":"ManualImport","importMode":"auto"}`, so they no longer require a manual Activity→Queue import. A **new Radarr client** brings the same fix to movies. Conservative by design: it ignores unparseable season packs ("No files are eligible for import") and Custom-Format rejections. New env: `RADARR_URL`, `RADARR_API_KEY`, and `AUTO_IMPORT_BLOCKED` (default on; each side gated on its own API key). DRY_RUN logs what it would import and fires nothing.
- **`test-alert --verify`.** The alert test command now reports per-channel delivery status (webhook HTTP status; email = accepted-by-server) with a ✓/✗ summary and a non-zero exit if any configured channel fails to send — so you can confirm the Discord/Slack/email path actually works, not just that it was attempted. The plain `test-alert` behavior is unchanged, and the production alert call-sites (token-failure / persistence / readonly) are untouched.

### Changed
- **`.env.example`** documents the new vars (`RADARR_*`, `AUTO_IMPORT_BLOCKED`, and the Tier 1 `INPROGRESS_BOOST_WINDOW_DAYS`).

### Notes
- SemVer: MINOR — backward-compatible features; auto-import is inert unless the relevant API key is set, and `--verify` is opt-in.
- 183 unit tests pass (35 new across the two features).

## [v1.7.0] - 2026-06-18

Tier 1 backlog: a daily-UX boost and a setup-safety command.

### Added
- **In-progress weekly priority boost.** When a new episode of a season you're actively watching lands in SABnzbd behind movies/other grabs, it's now bumped to **High** so it isn't stuck behind the queue for ~half a day. A new per-episode ledger (`episode_boosts`) tracks what's already been boosted, and the trigger is gated on a recency window — a tracked Jellyfin user must have played an episode of that season within `INPROGRESS_BOOST_WINDOW_DAYS` (new env, **default 7**, tuned for weekly-cadence shows; `<= 0` disables it). This complements the existing season-unlock boosts, which only fired on watch-progress/playback unlocks and never on routine weekly episodes of an already-unlocked season.
- **`validate` command** (`trakt_discovery.py validate`) — one read-only probe of every integration (Trakt, TMDB, Seerr, Sonarr, Jellyfin, SABnzbd, alert channels) that prints a ✓/✗ per service so a misconfigured `.env` is caught immediately instead of surfacing as silent zero-request cycles. Resolves each `JELLYFIN_USER_IDS` entry and flags display-names that should be UUIDs. Exits non-zero if a required service (Trakt/Sonarr/Jellyfin) fails; secrets are redacted from all output. Optional `--send` also fires a test alert through each channel.

### Notes
- SemVer: MINOR — two backward-compatible features; the boost is inert unless a show is actively in-progress within the window, and `validate` is a new opt-in command.
- 148 unit tests pass (35 new across the two features).

## [v1.6.1] - 2026-06-16

### Fixed
- **Docker image build unbroken.** `build-and-push` had been failing on every merge since v1.5.2 — a trailing `# python:3.11-slim` comment on the `FROM` line triggered `FROM requires either one or three arguments`. It slipped past PR checks because the image build only runs on merge to `main`, not on PRs. Consequence: GHCR `:latest` was stuck at v1.5.1, so the v1.5.2 and v1.6.0 code never actually deployed. The comment is moved to its own line above `FROM`; the digest pin is retained. First buildable release since v1.5.1.

## [v1.6.0] - 2026-06-15

### Changed
- **Concurrent webhook processing (de-serialized).** The redundant `webhook_lock` is removed. It was held across the entire webhook subprocess (which sleeps 15+10+20+30s), so a Trakt bulk-add of ~10 shows serialized into ~12 minutes. The atomic `claim_series_for_processing` (`INSERT … ON CONFLICT`, added in v1.5.0) already prevents same-series double-processing across subprocesses — which the in-memory lock never could — so the lock was pure overhead. Webhooks now run concurrently via `webhook_executor` (max_workers=3). The poll/playback/trakt locks are untouched.
- **In-process playback check.** The 45-second playback check no longer spawns a `media_automation.py playback` subprocess (~1900 cold starts/day); it runs inside the scheduler's playback thread. Done safely: parameter injection to avoid a circular import (`check_active_playback(conn, session=None, stop_event=None)`), interruptible `stop_event.wait()` for every playback-path sleep, a thread-confined `conn` + `requests.Session()`, an error boundary so a playback failure can't crash the scheduler, and a back-compatible `playback` CLI.

### Notes
- After deploy, the first 45s cycle should run playback in-process (no `Running: …playback` log lines) and still shut down gracefully in under ~2s.

## [v1.5.2] - 2026-06-15

### Security
- **TMDB API key redacted from logs.** `tmdb_get` sends the key as an `?api_key=` query param, so on any request failure the `requests` exception string carried the key into `docker logs` (same class as the SABnzbd fix in v1.5.0). A `_redact_secrets()` helper now masks `api_key=…` in the URL/exception across all logging branches. The v3 query-param flow is kept (switching to a Bearer token would require a v4 token).

### Changed
- **Base image pinned to its digest** (`python:3.11-slim@sha256:…`) for reproducible builds.

### Fixed
- **Silent TVDB-ID collisions** in `check_watch_progress`/`check_active_playback` now log a warning naming both series and keep the first, instead of silently last-write-wins.
- **`parse_env_list`** only treats `" #"` (space-then-hash) as an inline comment, so a legitimate token containing `#` survives while `value  # comment` still strips.

## [v1.5.1] - 2026-06-14

### Fixed
- **Concurrency-safe `processed_series.status` migration.** On first start of the v1.5 image against an existing DB, the poll and playback subprocesses both ran `PRAGMA table_info` then `ALTER TABLE … ADD COLUMN status` at the same instant, so the loser logged `duplicate column name: status`. It self-healed but logged an alarming error on every fresh-DB upgrade. The migration is now `_ensure_status_column`, which swallows the benign duplicate-column collision (re-raising any other `OperationalError`); legacy rows are still backfilled to `'done'`.

## [v1.5.0] - 2026-06-14

Deep-review reliability & security hardening — items 1–5 from the codebase audit. **94 unit tests pass.**

### Security
- **Secret redaction.** The SABnzbd API key (passed as a URL query param) and the alert-webhook token no longer reach logs on connection errors.

### Fixed
- **Atomic + durable series claim** *(core reliability)*. A cross-process `INSERT … ON CONFLICT` claim plus a `status` column (`in_progress` → `done`) with stale-claim recovery. A crashed or failed monitoring setup is now retried instead of permanently flagging a show whose monitoring never applied — the bug that caused full-library downloads. An idempotent `ALTER TABLE` migration runs on first start.
- **API/DB error contracts.** `None`-response guards at every Sonarr call site, a mass-delete plausibility guard in stale-DB cleanup, a media-side readonly-DB probe (skips cleanly and logs a chown hint), and HTTP-date `Retry-After` handling.
- **Scheduler/webhook hardening.** `ThreadingHTTPServer`, a request socket timeout, 400/413 body handling (Content-Length parse moved inside the try, 1 MiB cap), and SIGTERM forwarded to the child subprocess for clean shutdown.

### Changed
- **Container renamed `media-automation` → `cascade-media`**, with a port-aware healthcheck (Dockerfile + compose), a `RUN_CATCHUP_ON_START=false` default, and `mem_limit`/`cpus` limits.

### Upgrade notes
- ⚠ The container rename needs a one-time **stop → rm → recreate** (a plain restart won't adopt the new name). Keep the same data-volume mapping — the SQLite DB and Trakt token live on the host volume and survive the rename. Item 2's DB migration is automatic and idempotent; no manual step.

## [v1.4.0] - 2026-06-11

### Added
- **AI prompt is now aware of your platform blocklist.** `build_ai_prompt` receives `TMDB_DISALLOWED_NETWORKS` + `TMDB_DISALLOWED_PROVIDERS` and instructs Gemini not to suggest titles exclusive to those platforms. In live testing this was the single biggest improvement: a config blocking Netflix/Apple TV/Max/HBO went from **0 requests** (every AI pick landed on a blocked platform) to a **fully-filled budget** of acclaimed, on-platform, taste-matched titles.
- `AI_TIMEOUT_SECONDS` (default **300**, was a hardcoded 60) — configurable for slower/Pro models.
- `AI_SUGGESTIONS_MULTIPLIER` (default **3**, was a hardcoded 2) — more candidates through the filters.
- `AI_MIN_RATING` / `AI_MIN_VOTES` (default to the global `TRAKT_MIN_RATING` / `TRAKT_MIN_VOTES`) — optional AI-source-only floors so brand-new trending titles, which lag on Trakt vote counts, can pass without loosening the other lists.

### Fixed
- **AI title resolution now prefers an exact match.** `resolve_ai_suggestion` previously took the first Trakt result passing a loose startswith test, so *Sugar* resolved to the higher-ranked *Sugar Apple Fairy Tale*. It now scans for an exact normalized-title match first and only falls back to the prefix rule when none exists.

### Notes
- ⚠ A grounded call asking for more candidates can take ~70s+ (Pro models longer). `AI_TIMEOUT_SECONDS=300` now needs headroom under the scheduler: set `TRAKT_SCRIPT_TIMEOUT=600` (documented in `.env.example`), otherwise the run is killed mid-cycle.
- SemVer: MINOR — new optional config + improvements; defaults preserve v1.3.0 behavior except the resolver fix (a strict improvement) and the blocklist-aware prompt (only active when you have disallowed networks/providers set).

## [v1.3.0] - 2026-06-10

### Added
- **AI-powered discovery source (`"ai"`) for `trakt_discovery.py`.** Add `ai` to `TRAKT_LISTS` (typically first) and set `GEMINI_API_KEY` to have Google Gemini nominate ranked show/movie candidates each cycle. The model receives a taste profile built from your Trakt watch history (titles, years, genres, play counts), Trakt + TMDB trending lists, and — with `AI_WEB_SEARCH=true` — live Google Search grounding on what's currently popular across streaming platforms. Suggestions are resolved to real Trakt/TMDB IDs via a two-pass `/search` lookup (exact-year first, then year-relaxed) and then flow through the **existing filter pipeline unchanged** — rating, votes, year, genre, content-rating, TMDB, Seerr dedup, and watch-history checks all still apply; the AI only nominates candidates.
- New env vars: `GEMINI_API_KEY`, `AI_MODEL` (default `gemini-flash-latest`, a floating alias that tracks Google's latest stable Flash model), `AI_WEB_SEARCH` (default true), `AI_HISTORY_ITEMS` (default 50).
- New `trakt_search()` helper (public `/search/{type}` endpoint) and a lenient JSON parser for grounded model output (Gemini's JSON mode is incompatible with Google Search grounding, so the output contract is prompt-enforced).
- 25 unit tests in `tests/test_ai_discovery.py` covering JSON parsing, title→ID resolution, per-cycle caching, request shape, and the failure path.

### Notes
- **Zero behavior change when unconfigured.** Without `ai` in `TRAKT_LISTS`, nothing differs from v1.2.4. With `ai` listed but no `GEMINI_API_KEY`, the source logs one warning and is skipped.
- **Fail-loud fallback:** any Gemini failure (bad key, deprecated model, quota, network) logs an error, fires one alert per cycle through the existing webhook/email channels, and discovery falls through to the remaining `TRAKT_LISTS` sources.
- One combined Gemini call per cycle covers both shows and movies (~1 call/day at default scheduling — well under typical free-tier limits; grounded calls have a separate daily free-tier cap).
- A grounded Gemini call can take 10–30 s; raise `TRAKT_SCRIPT_TIMEOUT` if your cycle already runs near the 300 s default.
- SemVer: MINOR bump — new backward-compatible functionality, no breaking changes.

## [v1.2.4] - 2026-05-25

### Reverted
- **`cancel_sonarr_auto_search` (added in v1.2.3) removed.** It hit `409 Conflict` on every real-world attempt: Sonarr refuses to `DELETE /api/v3/command/{id}` on commands in `started` status, and `MissingEpisodeSearch` transitions `queued → started` in under one second — faster than any webhook handler can react. The function's 7 unit tests are also removed. Verified the failure mode with Marvel's The Punisher (2 seasons) and Hacks (5 seasons): both leaked despite the cancel function firing, both produced `409 Conflict` warnings in the logs.

### Changed (operational)
- **REQUIRED setup:** Jellyseerr/Overseerr → Settings → Services → Sonarr → uncheck "Enable Automatic Search" on every Sonarr server. Without this, multi-season manual ("Request all seasons") submissions over-grab the entire show because Sonarr fires its own `MissingEpisodeSearch` immediately at series-add time. With it disabled, cascade-media's explicit `SeasonSearch` (target season) and `EpisodeSearch` (preview E01s) at T+17 s are the only searches that run.
- README setup checklist and SYSTEM_DOCUMENTATION.md both gain a "Required external configuration" section documenting the Jellyseerr setting and explaining why it's necessary.

### Notes
- v1.2.2's bulk-PUT and season-level-first reorder work in `apply_monitoring` is retained — those changes are correct on their own merits (faster, lighter on Sonarr's API, fewer race-window seconds even if the race itself is now closed at a different layer).
- Cost of the Jellyseerr config change: ~15 seconds extra delay between Seerr "Request" and first NZB grab (cascade-media's `SeasonSearch` fires after a 15 s settle wait). Movies via Radarr are unaffected.
- Validated 2026-05-26 00:54 UTC with Rivals (2024) on Jellyseerr v3.2.0: zero `MissingEpisodeSearch` in Sonarr's command queue for the new series, 9 grabs total (8 S1 + 1 S2E01 preview), all `releaseSource: UserInvokedSearch`.

## [v1.2.3] - 2026-05-25

> **Note (added 2026-05-25 evening, see v1.2.4):** The cancel-command approach this release introduced **does not work** in production. Sonarr returns `409 Conflict` for any `DELETE /api/v3/command/{id}` against a command in `started` status, and `MissingEpisodeSearch` transitions `queued → started` in under one second — faster than the webhook handler can react. The function is reverted in v1.2.4 and the actual fix moved to the Jellyseerr config layer.

### Fixed
- **Cascade over-grab race (real root cause).** v1.2.2 narrowed the API loop from ~4s to ~250ms believing Sonarr's auto-search-on-add re-checked episode `monitored` state during execution. It does not. `MissingEpisodeSearch` snapshots every monitored episode ID at command-queue time — which happens the same second the series is created with Sonarr's default-all-monitored — and serially grabs them regardless of subsequent monitor flips. v1.2.2 left correct episode-level state but Sonarr kept grabbing the captured IDs over the next ~90 seconds.
  - Reproduced 2026-05-25 with *Euphoria (US)*: 175 ms monitor update, every grab still on the original snapshot. S2E02–E08 and S3E02–E07 all leaked.

### Added
- **`cancel_sonarr_auto_search(series_id, title)`** queries `/api/v3/command`, finds any `MissingEpisodeSearch` or `SeriesSearch` whose `body.seriesId` matches the just-added series and whose `status` is `queued` or `started`, and `DELETE`s each. Called as the FIRST API action in `process_new_series`, before any monitoring work. The webhook arrives the same second Sonarr queues the command and Sonarr's serial search loop runs for ~60–90 seconds before reaching non-target episodes, so this DELETE consistently lands in time.
- 7 unit tests for the cancel function (correct series filter, only cancellable statuses, ignores unrelated command types, empty queue, DRY_RUN).

### Notes
- Cancelling a search command does NOT undo grabs that have already posted NZBs to SABnzbd. With the cancel running as the first API call (~200 ms into the webhook), no grabs have happened yet, so nothing is left to clean up.
- The v1.2.2 bulk-PUT + reorder work is retained — it makes the subsequent episode-monitor flips and the 15s/10s/20s/30s re-apply passes much faster, and is correct independent of the auto-search race.

## [v1.2.2] - 2026-05-25

> **Note (added 2026-05-25 evening):** This release reduced the race window but did NOT close it — Sonarr's `MissingEpisodeSearch` snapshots episode IDs at queue time and does not re-check `monitored` state during execution. v1.2.3 ships the actual fix (cancel the search command outright). v1.2.2's bulk-PUT and reorder work is retained because it's correct on its own merits.

### Fixed
- **Cascade monitoring race against Sonarr's auto-search-on-add.** New TV series with many seasons could end up with full downloads of every season instead of the intended "Season 1 full + E01 previews" cascade. The original `apply_monitoring` looped one `PUT /api/v3/episode/{id}` per episode (~80ms each), which took ~4 seconds on a 56-episode series. Sonarr's auto-search command — fired by Seerr's series-add — enumerated monitored episode IDs at ~T+0.1s and pushed NZBs to SABnzbd before that loop could complete; once an NZB is in SABnzbd, neither monitor-flag changes nor Sonarr-queue cleanup can call it back. Reproduced 2026-05-25 with "Killer Cases" (S1 + S2E01 correct, S3–S7 every episode wrong).

### Changed
- **`apply_monitoring` now uses Sonarr's bulk `PUT /api/v3/episode/monitor` endpoint**, collapsing N per-episode PUTs into at most 2 bulk calls. The series-level `PUT /api/v3/series/{id}` (season-level `monitored` flags) now runs FIRST, before any episode work, so Sonarr stops auto-searching unmonitored seasons as quickly as possible. Race window shrinks from ~4s to ~250ms.

### Added
- First test coverage for `media_automation.py`: 7 unit tests covering `determine_target_season` (all-seasons / specific-seasons / Seerr "remaining" / files-exist / fallback) and `apply_monitoring` (bulk-endpoint usage, call ordering, no-op skip). Closes the test-coverage gap flagged in STATUS.md.

### Notes
- No environment-variable changes. No deployment-procedure changes beyond `docker compose pull && up -d`.
- Existing series unaffected — fix only changes the API call shape during new-series setup. The 15s wait + 3-pass re-apply cleanup remains as defense in depth.

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

[v1.8.1]: https://github.com/normalee1993/cascade-media/releases/tag/v1.8.1
[v1.8.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.8.0
[v1.7.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.7.0
[v1.6.1]: https://github.com/normalee1993/cascade-media/releases/tag/v1.6.1
[v1.6.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.6.0
[v1.5.2]: https://github.com/normalee1993/cascade-media/releases/tag/v1.5.2
[v1.5.1]: https://github.com/normalee1993/cascade-media/releases/tag/v1.5.1
[v1.5.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.5.0
[v1.4.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.4.0
[v1.3.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.3.0
[v1.2.4]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.4
[v1.2.3]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.3
[v1.2.2]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.2
[v1.2.1]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.1
[v1.2.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.2.0
[v1.1.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.1.0
[v1.0.0]: https://github.com/normalee1993/cascade-media/releases/tag/v1.0.0
