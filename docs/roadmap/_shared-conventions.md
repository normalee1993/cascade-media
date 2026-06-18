# Shared conventions (cascade-media)

> Inlined into every phase handoff file. Read this first if you're a fresh session.

- **Codebase:** 3 flat Python files at repo root — `scheduler.py` (entrypoint/orchestration), `media_automation.py` (TV cascade lifecycle), `trakt_discovery.py` (discovery). Stdlib + `requests` + `tzdata` only. SQLite (WAL mode) lives at the host `DATA_DIR` volume, not in the container.
- **Tests:** stdlib `unittest` under `tests/`. **There is no python in the Claude sandbox** — run the suite via the project image:
  ```
  docker run --rm -e PYTHONDONTWRITEBYTECODE=1 \
    -v <worktree>:/code:ro -w /code \
    --entrypoint python ghcr.io/normalee1993/cascade-media:latest \
    -m unittest discover -s tests
  ```
  To dry-run modified code without rebuilding, mount the changed file over the image:
  `-v <worktree>/media_automation.py:/app/media_automation.py:ro`.
- **Manual live checks:** always set `DRY_RUN=true`, e.g.
  `docker exec -e DRY_RUN=true cascade-media python -u /app/<script>.py <cmd>`.
- **Releases:** after a PR merges to `main`, push **one** SemVer tag `vX.Y.Z`. CI (`.github/workflows/docker-build.yml`) auto-builds the GHCR image + a GitHub Release on `v*` push. PATCH = fix, MINOR = backward-compat feature, MAJOR = breaking. **Push tags one at a time** — pushing >3 tags at once doesn't trigger CI.
- **CI gotcha:** PRs run the `test` job only; the Docker `build-and-push` runs **only on merge to main**. For any Dockerfile/build change, run a local `docker build` before merge, and after merging confirm `build-and-push` went green (`gh run list`) before claiming anything is deployed — a healthy container ≠ new code.
- **Path/Docker gotcha:** `/home/claude` maps to `/mnt/user` inside the Claude env, but the Docker daemon runs outside that namespace. Never run `docker compose` from `/home/claude` paths.
- **Container name** is `cascade-media`. **Production deploys** (container recreate, edits to the production `.env`, flipping `TRAKT_LISTS`) are done by the user, not the agent.
- **Git:** the user is learning git/gh — briefly explain commands as you run them. Commit messages end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
