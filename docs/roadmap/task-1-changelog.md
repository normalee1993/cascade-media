# Task 1 — Backfill CHANGELOG.md (v1.5.0 → v1.6.1)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Why
The repo has shipped **v1.5.0, v1.5.1, v1.5.2, v1.6.0, v1.6.1** (PRs #15–#19, all tagged + released on GitHub) but `CHANGELOG.md` stops at **v1.4.0**. Five releases — including the deep-review security/reliability pass and the concurrency rework — are undocumented in the changelog.

## What to do
In `CHANGELOG.md`, prepend five entries **above** the `## [v1.4.0] - 2026-06-11` heading, in the existing Keep-a-Changelog style (use `### Added/Changed/Fixed/Security/Notes` subsections as appropriate). Then add the matching `[vX.Y.Z]: https://github.com/normalee1993/cascade-media/releases/tag/vX.Y.Z` link references at the bottom, above the `[v1.4.0]:` line.

Source material (PR bodies + tag annotations). Write user-facing prose, not a raw paste:

- **## [v1.6.1] - 2026-06-16 — Fixed (PR #19):** Docker image build had been failing on every merge since #17 — a trailing `# python:3.11-slim` comment on the `FROM` line triggered "FROM requires either one or three arguments". It slipped past PR checks because `build-and-push` only runs on merge to main. Result: GHCR `:latest` was stuck at v1.5.1, so the #17 and #18 code never actually deployed. Fix: moved the comment to its own line above `FROM`; digest pin retained. First buildable release since v1.5.1.
- **## [v1.6.0] - 2026-06-15 — Changed / Performance (PR #18):**
  - *WI-1:* removed the redundant `webhook_lock`. It was held across the whole webhook subprocess (15+10+20+30s of sleeps), serializing a Trakt bulk-add of ~10 shows into ~12 min. The atomic `claim_series_for_processing` (INSERT…ON CONFLICT, added in v1.5.0) already prevents same-series double-processing across subprocesses, so the lock was pure overhead. Webhooks now run concurrently via `webhook_executor` (max_workers=3).
  - *WI-2:* moved the 45s playback check in-process (eliminates ~1900 subprocess cold-starts/day) — parameter injection to avoid circular import, interruptible `stop_event.wait()` for all playback-path sleeps, thread-confined conn + requests.Session, an error boundary so a playback failure can't crash the scheduler, and a back-compatible `playback` CLI.
- **## [v1.5.2] - 2026-06-15 — Security / Fixed (PR #17):** Redact TMDB `api_key=` query param from logs (same class as the v1.5.0 SABnzbd fix, via a `_redact_secrets` helper across all four logging branches; kept the v3 query-param flow). Pinned base image to its digest (`python:3.11-slim@sha256:…`). Silent TVDB-ID collisions in `check_watch_progress`/`check_active_playback` now log a warning naming both series and keep the first. `parse_env_list` only treats `" #"` (space-then-hash) as an inline comment so tokens containing `#` survive.
- **## [v1.5.1] - 2026-06-14 — Fixed (PR #16):** Concurrency-safe `processed_series.status` migration. On first start of the v1.5 image against an existing DB, the poll and playback subprocesses raced the `PRAGMA`-check-then-`ALTER`, so the loser logged `duplicate column name: status`. Extracted `_ensure_status_column` which swallows the benign duplicate-column collision (re-raising any other `OperationalError`); legacy rows still backfilled to `'done'`.
- **## [v1.5.0] - 2026-06-14 — Security / Reliability (PR #15):** Deep-review remediation, items 1–5:
  - Secret redaction — SABnzbd API key (URL query param) and alert-webhook token no longer reach logs on connection errors.
  - Atomic + durable series claim — cross-process `INSERT…ON CONFLICT` claim plus a `status` column (`in_progress`→`done`) with stale-claim recovery; fixes the bug where a crashed/failed monitoring setup permanently flagged a show and caused full-library downloads. Idempotent `ALTER TABLE` migration on first start.
  - API/DB error contracts — `None`-response guards at every Sonarr call site, a mass-delete plausibility guard in stale-DB cleanup, a media-side readonly-DB probe (skips + logs a chown hint), and HTTP-date `Retry-After` handling.
  - Scheduler/webhook hardening — `ThreadingHTTPServer`, request socket timeout, 400/413 body handling (Content-Length parse inside the try, 1 MiB cap), SIGTERM forwarded to the child subprocess.
  - Config & deployment — **container renamed `media-automation` → `cascade-media`** (⚠ one-time **stop → rm → recreate**, keep the same data-volume mapping; a plain restart won't adopt the new name; DB + token survive), port-aware healthcheck, `RUN_CATCHUP_ON_START=false` default, `mem_limit`/`cpus`.

## Verify
- Version headers are in descending order; dates match the tag dates.
- Every new `## [vX.Y.Z]` has a matching bottom link ref and the URL resolves.
- Docs-only change — no image rebuild or redeploy needed. This commit can be folded into Phase 1's PR or shipped on its own (no new tag required; it documents already-released versions).
