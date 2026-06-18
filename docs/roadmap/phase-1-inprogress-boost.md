# Phase 1 (Tier 1a) — In-progress weekly priority boost → tag v1.7.0 (MINOR)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Problem / why
When a new episode of a season the user is **actively watching** lands in SABnzbd behind a queue of movies/other grabs, it can sit ~half a day. The user wants such episodes bumped to **High** so they aren't waiting.

Today this never happens: `boost_season_priority` (`media_automation.py:1349`) only fires on *season-unlock* events — next-season unlock at `check_user_progress` (`media_automation.py:1230`) and Jellyfin E01-playback unlock of a still-*locked* season at `check_active_playback` (`media_automation.py:1637`). The `priority_boosts` table (`media_automation.py:423`) is keyed `(sonarr_id, season_number)`, and `is_season_boosted` (`media_automation.py:1326`) short-circuits any season already handled. So routine weekly episodes of an already-unlocked, in-progress season are never boosted.

## Decision (from the user)
Trigger = a tracked Jellyfin user has **played an episode of that season within a 7-day window** (weekly-cadence shows). This is the abandoned-show guard: a show last watched 8+ days ago does not get boosted.

## Design
All changes in `media_automation.py`.

1. **Per-episode ledger.** In `init_db` (`media_automation.py:380`) add:
   ```sql
   CREATE TABLE IF NOT EXISTS episode_boosts (
       sonarr_id INTEGER,
       season_number INTEGER,
       episode_number INTEGER,
       boosted_at TEXT,
       PRIMARY KEY (sonarr_id, season_number, episode_number)
   )
   ```
   Add `is_episode_boosted(conn, sonarr_id, season, ep)` and `mark_episode_boosted(conn, sonarr_id, season, ep)` mirroring the season helpers at `media_automation.py:1326`–`1347`. **Leave the season-level `priority_boosts` table and all existing boost logic untouched.**

2. **7-day recency signal.** Extend the Jellyfin watched query in `check_user_progress` (`media_automation.py:1097`) to include user-data so each played episode carries a last-played timestamp — add `UserData` (and/or `DatePlayed`) to the `Fields` param. From the returned items, compute, per (series, season), the **most-recent play time**. A season counts as "active" if that max is within `INPROGRESS_BOOST_WINDOW_DAYS` of now (new env var, **default 7**; read with the existing `get_int_env` helper).

3. **New `boost_in_progress_episodes(conn, ...)`.** Reuse the per-user, per-season watched data `check_user_progress` already assembles (`series_progress` → matched `sonarr_series`). For each season where: the user has ≥1 played episode, the most-recent play is **in-window**, the season is **unlocked / in-progress** (`is_season_unlocked` true), and it is **not** the next-season-unlock case already handled — scan the Sonarr queue exactly like `boost_season_priority` does (`media_automation.py:1409-1442`): `GET /queue?page=1&pageSize=200&includeUnknownSeriesItems=false`, match records by `seriesId` + season, map `downloadId` → episode number, then match SABnzbd slots via `sabnzbd_get_queue` and `nzo_id`. For each queued episode **not already in `episode_boosts`**, call `sabnzbd_set_priority(nzo_id, 1)` (High) and `mark_episode_boosted`. Reuse `sabnzbd_get_queue`/`sabnzbd_set_priority` (`media_automation.py:265,273`). Honor `DRY_RUN` (log "would set…", make no calls, don't write the ledger) and short-circuit if `SABNZBD_API_KEY` is unset — match the patterns already in `boost_season_priority`.

   Implementation choice: factor the "find queued episodes for a season" logic so it can serve both per-episode (this) and the existing season boost, OR keep it self-contained — either is fine; prefer reuse without disturbing the season path's behavior.

4. **Wire-in.** Call `boost_in_progress_episodes(conn)` from `run_once` (`media_automation.py:1710`) right after `check_watch_progress(conn)`. The 15-min poll cadence is well within a 7-day window. Wrap in the run's existing try/except so a failure can't abort the cycle.

## New env
`INPROGRESS_BOOST_WINDOW_DAYS` (default 7). Documented for real in Phase 4's `.env.example` pass; add an inline mention near the other boost/SAB envs now.

## Files
- `media_automation.py` (schema + helpers + new function + wire-in)
- new `tests/test_boost.py` (or extend `tests/test_filters.py`)

## Tests (stdlib unittest, reuse the fake-`requests`/in-memory-sqlite patterns already in `tests/`)
- `episode_boosts` set + idempotency (`is_/mark_episode_boosted`).
- In-window active season → the new queued episode is boosted (asserts `sabnzbd_set_priority` called with priority 1 + ledger written).
- Last play 8+ days ago → **not** boosted.
- Season still locked → skipped.
- Episode already in `episode_boosts` → skipped (no duplicate boost).
- `DRY_RUN=true` → no `sabnzbd_set_priority` call, no ledger write.
- `SABNZBD_API_KEY` unset → no-op.

## Verify
- Full suite green via the docker-run command in `_shared-conventions.md`.
- Live dry-run: `docker exec -e DRY_RUN=true cascade-media python -u /app/media_automation.py playback` plus a poll run; confirm logs show in-progress episodes flagged for boost on an actively-watched weekly show, and skipped on a show last watched >7 days ago.
- Open PR, merge, then tag **v1.7.0** (MINOR — new backward-compatible feature; default behavior unchanged unless a show is actively in-progress).
