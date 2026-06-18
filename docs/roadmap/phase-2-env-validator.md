# Phase 2 (Tier 1b) — `.env` connection validator → tag v1.8.0 (MINOR)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Problem / why
Misconfiguration in `.env` (wrong URL, empty secret, display-name instead of a Jellyfin UUID, expired token) currently surfaces as silent zero-request discovery cycles — hard to diagnose. This has bitten the project repeatedly. Provide **one command** that probes every configured integration and reports a clear ✓/✗ so setup/misconfig problems are caught immediately. (This connection-test layer is also the reusable foundation for the future Web GUI.)

## Design
All changes in `trakt_discovery.py`.

1. **New `validate` subcommand.** Add to the `main()` dispatch (`trakt_discovery.py:1858`) alongside `status`/`discover`/`test-alert`, and to the usage string. Implement `cmd_validate(conn)` modeled on `cmd_status` (`trakt_discovery.py:1756`) and `cmd_test_alert` (`trakt_discovery.py:1833`).

2. **Per-service probe.** For each service do a cheap, read-only call and print a line: `✓ <Service>: <detail>` or `✗ <Service>: <reason>`. Services + the existing helper to reuse:
   - **Trakt** — token via `load_tokens`/`get_valid_token` (`trakt_discovery.py:619,706`) + a `trakt_get` ping (e.g. a trivial authed endpoint). Report token validity + days-to-expiry (reuse the math in `cmd_status`).
   - **TMDB** — only if `TMDB_API_KEY` set: `tmdb_get` a config/trivial endpoint (`trakt_discovery.py:394`).
   - **Seerr** — `seerr_get` a status/settings endpoint (`trakt_discovery.py:371`).
   - **Sonarr** — `GET /system/status`. There's no Sonarr client in `trakt_discovery.py`; replicate the tiny GET helper from `media_automation.py:190` (build URL from `SONARR_URL` + `X-Api-Key: SONARR_API_KEY`), or add a minimal local helper. Report the Sonarr version on success.
   - **Jellyfin** — `GET /System/Info` (server reachable + version), then resolve **each** entry in `JELLYFIN_USER_IDS`: a value that is not a UUID (e.g. a display name like `azure-aperture`) should be flagged ✗ with a hint that Jellyfin needs the user **UUID**, not the display name. (This is a known prior failure mode.)
   - **SABnzbd** — a `version` or `queue` ping using `SABNZBD_URL` + `SABNZBD_API_KEY`.
   - **Alerts** — reuse `cmd_test_alert` plumbing: report which channels are configured (webhook + SMTP/email). Default: report only. Optional `--send` flag actually fires a test message through each.

3. **Redaction + exit code.** Pass any echoed URL or exception through `_redact_secrets` (`trakt_discovery.py:221`) so keys never print. Track required vs optional: **required = Trakt, Sonarr, Jellyfin** (core to the cascade); if any required service fails, `sys.exit(1)`. If only optional services (TMDB/Seerr/SABnzbd/alerts) are unset, exit 0 but print an informational note. Each probe must catch its own exceptions so one failure doesn't abort the rest.

## Files
- `trakt_discovery.py` (dispatch + `cmd_validate` + minimal Sonarr/Jellyfin/SAB probes)
- new `tests/test_validate.py`

## Tests (stdlib unittest, fake `requests`)
- Each service renders ✓ on a good faked response and ✗ on failure/timeout.
- Jellyfin user-ID check: UUID passes, display-name value is flagged.
- Exit-code logic: a required-service failure → non-zero; only-optional-unset → zero.
- `_redact_secrets` applied (no key substring in printed output).

## Verify
- Full suite green via the docker-run command in `_shared-conventions.md`.
- Live: `docker exec cascade-media python -u /app/trakt_discovery.py validate` against the running stack — expect every line ✓ and exit 0. (Optionally `validate --send` to confirm alert delivery.)
- Open PR, merge, then tag **v1.8.0** (MINOR — new command, no behavior change to existing paths).
