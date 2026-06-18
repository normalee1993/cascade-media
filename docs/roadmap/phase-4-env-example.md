# Phase 4 (Tier 2b) — Improve `.env.example` → tag v1.9.1 (PATCH, docs)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Problem / why
Setup is error-prone because `.env.example` (231 lines today) lacks concrete format examples for several list/format-sensitive vars, and the vars added in Phases 1–3 aren't documented. Past incidents trace to exactly this (Apple TV rebrand, HBO≠Max, Jellyfin display-name-vs-UUID).

## What to do
Edit `.env.example` only (docs-only change).

1. **Add real, copy-pasteable examples** (as comments) for the format-sensitive vars:
   - `TRAKT_YEARS` — show the exact accepted format (e.g. range vs list).
   - Genre / language list vars — example values + that they're comma-separated.
   - `TMDB_DISALLOWED_NETWORKS` / `TMDB_DISALLOWED_PROVIDERS` — example `Netflix, Apple TV, Max, HBO` with the two gotchas called out: **Apple TV has no "+"** (TMDB rebrand), and **HBO and Max are separate** networks (list both to block both).
   - `JELLYFIN_USER_IDS` — must be Jellyfin **UUIDs**, not display names; show a UUID-shaped example and a one-line note on where to find it.
2. **Document the new env vars** from Phases 1–3 with defaults:
   - `INPROGRESS_BOOST_WINDOW_DAYS` (default 7) — Phase 1.
   - `RADARR_URL`, `RADARR_API_KEY`, `AUTO_IMPORT_BLOCKED` — Phase 3.
3. **Respect `parse_env_list` semantics** — only `" #"` (space-then-hash) is treated as an inline comment, so don't write example values where a literal `#` in a token would be silently stripped; verify any example with a `#` still parses as intended.

## Verify
- Spin a fresh container (or `docker run` with these example values) under `DRY_RUN=true` and run the Phase 2 validator: `docker exec cascade-media python -u /app/trakt_discovery.py validate` — all example-derived values parse cleanly with no warnings.
- Open PR, merge, then tag **v1.9.1** (PATCH — docs/config only).
