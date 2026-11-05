"""Small, dependency-free security helpers shared by every entrypoint.

Kept in one module so scheduler.py, media_automation.py, trakt_discovery.py and
taste_profile.py redact, clamp and sanitize the same way. Pure functions only —
no logging configuration, no env reads — so importing it has no side effects.
"""

import re

# Query-string credentials: TMDB's ?api_key=, SABnzbd's ?apikey=, the webhook's
# ?token=, OAuth-style access/refresh tokens and any ?password=.
_QUERY_SECRET_RE = re.compile(
    r"(?i)(\b(?:api_?key|access_token|refresh_token|token|password)=)[^&\s'\"<>]+"
)
# URL userinfo: scheme://user:pass@host → scheme://REDACTED@host
_USERINFO_RE = re.compile(r"(?i)(\b[a-z][a-z0-9+.\-]*://)[^/\s@:'\"<>]+(?::[^/\s@'\"<>]*)?@")

# C0/C1 control characters (incl. ESC for ANSI/OSC terminal sequences) and the
# Unicode line/paragraph separators that some viewers treat as newlines.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f  ]")


def redact(value):
    """Mask credentials in a string (or anything str()-able).

    Covers query-string secrets (api_key/apikey/token/...) and URL userinfo, so a
    configured SONARR_URL=http://admin:pw@host or a TMDB URL embedded in a
    requests exception never reaches logs, alerts or validate output verbatim.
    """
    text = str(value)
    text = _QUERY_SECRET_RE.sub(r"\1REDACTED", text)
    return _USERINFO_RE.sub(r"\1REDACTED@", text)


def redact_exception(exc):
    """Rewrite an exception's args in place with redacted text, then return it.

    requests/urllib3 exception messages embed the full request URL (including
    TMDB's ?api_key=). Callers that re-raise would otherwise hand the raw secret
    to every upstream log line and alert body. Rewriting args keeps the exception
    type (so `except requests.exceptions.Timeout` still works) while making
    str(exc) safe. Best-effort: never raises.
    """
    try:
        exc.args = tuple(redact(a) if not isinstance(a, (int, float)) else a for a in exc.args) or (redact(exc),)
    except Exception:
        pass
    return exc


def clamp_retry_after(raw, default=60, maximum=120):
    """Parse a server-supplied Retry-After into a safe sleep length in seconds.

    RFC 7231 allows delay-seconds or an HTTP-date; we honor only the numeric
    form. Anything non-numeric falls back to `default`. The result is clamped to
    [0, maximum] so a hostile or buggy upstream can't make time.sleep() raise
    (negative / overflow) or stall a loop for years.
    Returns (seconds, was_numeric).
    """
    try:
        seconds = int(str(raw).strip())
        numeric = True
    except (TypeError, ValueError):
        seconds = default
        numeric = False
    return max(0, min(seconds, maximum)), numeric


def same_host_redirect(url, location):
    """Resolve a redirect Location against `url` and return it only if it is safe
    to follow with our API-key headers attached: same hostname, and either the
    same scheme or an http->https upgrade. Returns None otherwise.

    requests strips only the Authorization header on a cross-host redirect, so a
    custom X-Api-Key / X-Emby-Token header would be forwarded to any host a 3xx
    names. Callers disable automatic redirects and use this instead.
    """
    from urllib.parse import urljoin, urlsplit

    target = urljoin(url, location)
    src, dst = urlsplit(url), urlsplit(target)
    if not dst.hostname or (dst.hostname or "").lower() != (src.hostname or "").lower():
        return None
    if dst.scheme != src.scheme and not (src.scheme == "http" and dst.scheme == "https"):
        return None
    return target


def tighten_file_mode(path, mode=0o660):
    """Best-effort chmod of a file and its SQLite WAL/SHM side files.

    The DB holds the Trakt access + refresh tokens and household viewing data;
    files created before v1.11.0 (or under a permissive umask) may be 0644.
    Group stays rw so Unraid's 'users' group keeps access. Never raises.
    """
    import os

    for candidate in (path, f"{path}-wal", f"{path}-shm"):
        try:
            if os.path.isfile(candidate) and (os.stat(candidate).st_mode & 0o777) != mode:
                os.chmod(candidate, mode)
        except OSError:
            pass


def write_private_file(path, text, mode=0o640):
    """Write text to `path` without following a pre-planted symlink, with the
    file created as `mode` (not world-readable). Raises OSError on failure."""
    import os

    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, mode)
    try:
        os.fchmod(fd, mode)
    except (AttributeError, OSError):
        pass
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)


def safe_log(value, maxlen=200):
    """Render an untrusted value for a single log line.

    Control characters (newlines, ESC/ANSI, NUL...) are escaped so a webhook
    payload can't forge extra log lines or inject terminal sequences, and the
    result is truncated so one field can't flood the log.
    """
    text = str(value)
    if len(text) > maxlen:
        text = text[:maxlen] + f"...(+{len(text) - maxlen} chars)"
    return _CONTROL_RE.sub(lambda m: m.group(0).encode("unicode_escape").decode("ascii"), text)


def sanitize_prompt_text(value, maxlen=150):
    """Neutralize third-party text before it is placed in an LLM prompt.

    Titles, keywords and names come from TMDB/Trakt/Jellyfin/the arrs. Collapsing
    control characters and newlines to spaces stops one entry from starting a new
    top-level prompt line ("SYSTEM: ..."); the length cap bounds how much text a
    single entry can contribute. This reduces, but cannot eliminate, prompt
    injection — every pick is still re-filtered downstream.
    """
    text = _CONTROL_RE.sub(" ", str(value))
    text = " ".join(text.split())
    if len(text) > maxlen:
        text = text[:maxlen].rstrip() + "…"
    return text
