# Phase 3 (Tier 2a) — "Matched by ID" auto-import (Sonarr + Radarr) → tag v1.9.0 (MINOR)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Problem / why
Some completed downloads get stuck because the release name doesn't self-parse to a library title (e.g. `Battlestar.Galactica.2005.S02E01` vs `Battlestar Galactica (2003)`; movies like `Batman: The Long Halloween (2021)`). Sonarr/Radarr can only match them via grab-history **ID** and then refuse to auto-import as a safety measure — `importBlocked`/`importPending`, *"matched to series/movie by ID. Automatic import is not possible / Manual Import required."* This is documented at `SYSTEM_DOCUMENTATION.md:531`. Today they require a manual import in Activity → Queue. Automate it.

## Decision (from the user)
Cover **both Sonarr and Radarr**. Sonarr reuses the existing client; **Radarr has no client yet — build one.**

## Design
All changes in `media_automation.py`.

1. **Sonarr — `resolve_blocked_imports()`.** Page through the queue using the same pagination pattern as `cleanup_unwanted_queue_items` (`media_automation.py:970`): `GET /queue?page=N&pageSize=100&includeUnknownSeriesItems=false`. Select records whose tracked-download state indicates an import block **and** whose status message matches the by-ID signature (match on the distinctive phrase, e.g. contains "matched" + "by ID" / "Manual Import required"; be conservative). For each, `GET /api/v3/manualimport?downloadId=<downloadId>` to get the candidate file rows, build the import payload (carry through the resolved seriesId/episodeIds/quality the manualimport response provides), then `POST /api/v3/command {"name":"ManualImport","files":[…],"importMode":"auto"}`. Reuse `sonarr_get`/`sonarr_post` (`media_automation.py:190,200`).

2. **Radarr — new client + same flow.** Add `radarr_get`/`radarr_post` mirroring `sonarr_get`/`sonarr_post` (`media_automation.py:190-204`), driven by new env `RADARR_URL` + `RADARR_API_KEY` (read at module top next to the Sonarr config). Radarr is also `/api/v3` with `movieId` instead of `seriesId`/`episodeIds`. Implement `resolve_blocked_imports_radarr()` with the same blocked-queue → `manualimport?downloadId=` → `ManualImport` command flow.

3. **Wire-in + safety.** Call both from `run_once` (`media_automation.py:1710`), each gated on its `*_API_KEY` being set (and on an optional `AUTO_IMPORT_BLOCKED` master toggle, default on when keys present). Honor `DRY_RUN`: log the items it *would* import without firing the command. **Only act on records matching the by-ID message signature** — do not force genuinely-unparseable junk (e.g. the "No files are eligible for import" season-pack case, or benign "Not a Custom Format upgrade" rejections, must be ignored). Wrap in try/except so a failure can't abort the cycle.

## New env
`RADARR_URL`, `RADARR_API_KEY`, optional `AUTO_IMPORT_BLOCKED` (default on when the relevant keys are present). Document in Phase 4's `.env.example` pass.

## Files
- `media_automation.py` (Radarr client + both resolver functions + wire-in)
- tests (extend `tests/test_filters.py` or new `tests/test_autoimport.py`)

## Tests (stdlib unittest, fake `requests`)
- Message-signature matcher: acts on the by-ID block; **ignores** "No files eligible for import" and "Not a Custom Format upgrade".
- manualimport → ManualImport payload shape is correct (files carried through, `importMode: auto`).
- `DRY_RUN=true` → identifies items, fires no command.
- Radarr client request shape (URL, `X-Api-Key` header, `movieId`).
- No-op when `SONARR_API_KEY` / `RADARR_API_KEY` unset (and when `AUTO_IMPORT_BLOCKED` is off).

## Verify
- Full suite green via the docker-run command in `_shared-conventions.md`.
- Live dry-run with a known stuck item present: `docker exec -e DRY_RUN=true cascade-media python -u /app/media_automation.py` — confirm it identifies the by-ID blocks (Sonarr and Radarr) and lists them; a non-DRY run should clear them from the queue (check Activity → Queue).
- Open PR, merge, then tag **v1.9.0** (MINOR — new feature; off unless Radarr/Sonarr keys configured).
