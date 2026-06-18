# Phase 5 (Tier 2c) — Alert webhook end-to-end test → tag v1.9.2 (PATCH)

> Self-contained handoff. See `_shared-conventions.md` for repo/test/release conventions.

## Problem / why
The alert plumbing (Trakt token failure, persistence failure, readonly DB) fans out to a webhook (Discord/Slack/ntfy) and email. Today `test-alert` (`trakt_discovery.py:1833`) fires a message but doesn't **assert delivery** — there's no end-to-end confirmation that a configured channel actually accepted the message. Given that a silent alert failure once let watched items get re-downloaded for 9 days, the delivery path deserves a real check.

## Design
All changes in `trakt_discovery.py`.

1. **`test-alert --verify` mode.** Extend `cmd_test_alert` (`trakt_discovery.py:1833`) so that, with `--verify`, each channel's send reports success/failure with the HTTP status (where the transport exposes it — `_send_alert_webhook` at `trakt_discovery.py:343`, `_send_alert_email` at `trakt_discovery.py:314`). Print a per-channel ✓/✗ summary and exit non-zero if any configured channel failed to send. Without `--verify`, keep the current fire-and-forget behavior.
2. Have the send helpers surface a success boolean / status to the caller (small refactor) so `--verify` can report it, without changing their existing call sites' behavior.

## Files
- `trakt_discovery.py`
- tests (extend `tests/test_filters.py` or new `tests/test_alerts.py`)

## Tests (stdlib unittest, faked transports)
- Fan-out: with both webhook + email configured, both are attempted.
- Per-channel status reporting: a 2xx → ✓, a non-2xx/exception → ✗.
- Exit code: any configured-channel failure under `--verify` → non-zero; all success → zero.
- No channels configured → clear message + non-zero (matches current behavior).

## Verify
- Full suite green via the docker-run command in `_shared-conventions.md`.
- Live: `docker exec cascade-media python -u /app/trakt_discovery.py test-alert --verify` — each configured channel reports delivered; **user confirms receipt** in Discord/email (the one manual step that proves true end-to-end delivery).
- Open PR, merge, then tag **v1.9.2** (PATCH).
