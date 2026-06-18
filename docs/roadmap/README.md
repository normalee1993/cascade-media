# Roadmap handoff files

Self-contained execution specs for the post-v1.6.1 backlog. Each file repeats the shared conventions so it can be handed to a fresh chat session on its own. Build order is top to bottom; **one phase fully implemented + tested + tagged before the next.**

**Tier 1 shipped together as PR #20 → v1.7.0 (2026-06-18).** Phases 1 and 2 were built in parallel and released under one version, so the remaining phases are renumbered below (each its own tag).

| Order | File | Item | Tier | Tag | Status |
|---|---|---|---|---|---|
| 0 | [task-1-changelog.md](task-1-changelog.md) | Backfill CHANGELOG v1.5.0–v1.6.1 | — | (docs) | ✅ done |
| 1 | [phase-1-inprogress-boost.md](phase-1-inprogress-boost.md) | In-progress weekly priority boost | 1a | v1.7.0 | ✅ done |
| 2 | [phase-2-env-validator.md](phase-2-env-validator.md) | `.env` connection validator | 1b | v1.7.0 | ✅ done |
| 3 | [phase-3-auto-import.md](phase-3-auto-import.md) | "Matched by ID" auto-import (Sonarr + Radarr) | 2a | v1.8.0 | ✅ done |
| 4 | [phase-4-env-example.md](phase-4-env-example.md) | Improve `.env.example` | 2b | v1.8.0 | ✅ done |
| 5 | [phase-5-alert-e2e.md](phase-5-alert-e2e.md) | Alert webhook end-to-end test | 2c | v1.8.0 | ✅ done |

**Tier 2 shipped together as PR #21 → v1.8.0 (2026-06-18)**, same as Tier 1.

All planned backlog phases are complete. Web GUI is deferred to a future **v2.0.0** milestone, to be built on the connection-test layer from Phase 2 (`validate`).
