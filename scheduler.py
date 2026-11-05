#!/usr/bin/env python3
"""Scheduler with webhook support for media automation."""

import base64
import hmac
import time
import os
import logging
import signal
import socket
import subprocess
import sys
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlsplit
import json

import requests

from security_utils import safe_log

# The playback check now runs IN-PROCESS in playback_check_loop (no subprocess
# per interval). media_automation owns all the playback logic; the scheduler
# only owns the long-lived resources (one sqlite conn + one requests.Session)
# and the loop/error boundary. The dependency is one-directional: scheduler
# imports media_automation, media_automation MUST NEVER import scheduler.
import media_automation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S"
)
log = logging.getLogger("scheduler")

def get_int_env(key, default):
    """Get integer from environment with validation."""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        log.error(f"Invalid {key}='{os.getenv(key)}', must be an integer. Using default: {default}")
        return default

INTERVAL_MINUTES = get_int_env("RUN_INTERVAL_MINUTES", 15)
RUN_CATCHUP_ON_START = os.getenv("RUN_CATCHUP_ON_START", "false").lower() == "true"
WEBHOOK_PORT = get_int_env("WEBHOOK_PORT", 9191)
SCRIPT_TIMEOUT = get_int_env("SCRIPT_TIMEOUT_MINUTES", 30) * 60
PLAYBACK_CHECK_INTERVAL = get_int_env("PLAYBACK_CHECK_INTERVAL", 45)
# (PLAYBACK_SCRIPT_TIMEOUT removed: the playback check is now in-process, with
# interruptible bounded waits + per-call timeout=30, so the old subprocess
# hard-timeout no longer applies.)

# Trakt discovery
TRAKT_DISCOVERY_ENABLED = os.getenv("TRAKT_DISCOVERY_ENABLED", "false").lower() == "true"
TRAKT_DISCOVERY_TIME = os.getenv("TRAKT_DISCOVERY_TIME", "00:00")  # HH:MM local time
TRAKT_DISCOVERY_TZ_NAME = os.getenv("TRAKT_DISCOVERY_TZ", "UTC")
try:
    TRAKT_DISCOVERY_TZ = ZoneInfo(TRAKT_DISCOVERY_TZ_NAME)
except ZoneInfoNotFoundError:
    logging.getLogger("scheduler").error(f"Invalid TRAKT_DISCOVERY_TZ='{TRAKT_DISCOVERY_TZ_NAME}', falling back to UTC")
    TRAKT_DISCOVERY_TZ = ZoneInfo("UTC")
    TRAKT_DISCOVERY_TZ_NAME = "UTC"
try:
    _h, _m = TRAKT_DISCOVERY_TIME.split(":")
    TRAKT_DISCOVERY_HOUR = int(_h)
    TRAKT_DISCOVERY_MINUTE = int(_m)
except (ValueError, AttributeError):
    logging.getLogger("scheduler").error(f"Invalid TRAKT_DISCOVERY_TIME='{TRAKT_DISCOVERY_TIME}', defaulting to 00:00")
    TRAKT_DISCOVERY_HOUR, TRAKT_DISCOVERY_MINUTE = 0, 0
TRAKT_SCRIPT_TIMEOUT = get_int_env("TRAKT_SCRIPT_TIMEOUT", 300)

def next_discovery_run():
    """Return the next datetime for the scheduled discovery time in the configured timezone."""
    now = datetime.now(TRAKT_DISCOVERY_TZ)
    target = now.replace(hour=TRAKT_DISCOVERY_HOUR, minute=TRAKT_DISCOVERY_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


# Separate locks for polling vs playback vs trakt processing.
# Note: there is deliberately NO webhook_lock. Cross-process double-processing of
# the same series is already prevented by claim_series_for_processing in
# media_automation.py (atomic INSERT ... ON CONFLICT DO NOTHING with stale
# recovery), which an in-memory lock could never do across subprocesses. Holding
# a lock across the whole webhook subprocess only serialized unrelated series —
# turning a Trakt bulk-add burst of ~10 SeriesAdd webhooks into ~12 minutes — so
# the lock was removed. Webhooks now run concurrently via webhook_executor.
poll_lock = threading.Lock()
playback_lock = threading.Lock()
trakt_lock = threading.Lock()

# Signalled from SIGTERM/SIGINT handlers; loops poll this instead of sleeping blindly
shutdown_event = threading.Event()

# Live child subprocesses (Popen objects). The SIGTERM/SIGINT handler forwards the
# signal to these so a child mid-write (e.g. Sonarr monitoring flips) gets a prompt,
# clean stop within the container's stop_grace_period instead of being SIGKILLed.
# Guarded by _children_lock. set/discard are async-signal-safe enough for our use:
# the handler only iterates a snapshot and calls terminate() (a single kill(2)).
_children_lock = threading.Lock()
_live_children = set()

# Max request body we will read from a webhook client. Sonarr's SeriesAdd payloads
# are a few KiB; anything past this is almost certainly malformed/hostile.
MAX_WEBHOOK_BODY = 1 * 1024 * 1024  # 1 MiB
# Per-socket-operation timeout AND whole-connection deadline (seconds). The
# socket timeout alone resets on every recv, so a client dripping one byte per
# 29s would hold a handler forever; WebhookHandler.setup() also arms a timer
# that tears the connection down once this many seconds have elapsed in total.
WEBHOOK_REQUEST_TIMEOUT = 30
# Request line + headers may total at most this many bytes. Stock http.server
# accepts ~100 headers x 64 KiB (~6.5 MB raw, ~20 MB once parsed) per connection
# before our code runs; Sonarr's headers are well under 2 KiB. Over budget -> 431.
MAX_HEADER_BYTES = 32 * 1024
# Concurrent webhook connections. With the header budget above, each connection
# costs at most ~MAX_HEADER_BYTES + MAX_WEBHOOK_BODY, so the cap bounds total
# memory. Sonarr sends one webhook at a time; the HEALTHCHECK needs one more slot.
MAX_WEBHOOK_CONNECTIONS = max(2, get_int_env("MAX_WEBHOOK_CONNECTIONS", 8))
# Distinct series that may be waiting for a webhook worker. Re-sends of an
# already-queued series are deduplicated; beyond this the server answers 503 so
# a flood can't grow the executor's (unbounded) work queue without limit.
MAX_PENDING_WEBHOOKS = max(1, get_int_env("MAX_PENDING_WEBHOOKS", 50))
# Sonarr series ids are positive 32-bit ints.
MAX_SERIES_ID = 2**31 - 1

# Shared secret required on every webhook POST. Sonarr sends it either as the
# webhook connection's Basic-auth password (Settings > Connect > Webhook >
# Password) or as ?token=<secret> on the webhook URL. Unset = webhooks are
# REFUSED (403) — the 15-minute poll still picks up new series, just later.
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

# Thread pool for webhook handling
webhook_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="webhook")


def _run_tracked(cmd, timeout):
    """Run a child process, registering it so the shutdown handler can terminate it.

    Mirrors subprocess.run(cmd, timeout=timeout) semantics: returns the completed
    process (with .returncode), raises subprocess.TimeoutExpired if it overruns,
    and propagates other exceptions. The difference is that the live Popen is
    tracked in _live_children, so a SIGTERM/SIGINT received while we're blocked in
    .wait() forwards a terminate() to the child for a clean stop.
    """
    proc = subprocess.Popen(cmd)
    with _children_lock:
        _live_children.add(proc)
    try:
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Match subprocess.run's behavior: kill the child, reap it, re-raise.
            proc.kill()
            proc.wait()
            raise
        except BaseException:
            # On any other interruption (incl. KeyboardInterrupt), don't leak the child.
            proc.kill()
            proc.wait()
            raise
        return proc
    finally:
        with _children_lock:
            _live_children.discard(proc)


def run_poll_script(args=None):
    """Run the full polling script (set_initial_monitoring + check_watch_progress)."""
    if not poll_lock.acquire(blocking=False):
        log.info("Poll script already running, skipping")
        return False

    try:
        cmd = [sys.executable, "/app/media_automation.py"]
        if args:
            cmd.extend(args)
        log.info(f"Running: {' '.join(cmd)}")
        result = _run_tracked(cmd, timeout=SCRIPT_TIMEOUT)
        if result.returncode != 0:
            log.error(f"Script exited with code {result.returncode}")
            return False
        return True
    except subprocess.TimeoutExpired:
        log.error(f"Script timed out after {SCRIPT_TIMEOUT}s")
        return False
    except Exception as e:
        log.error(f"Failed to run script: {e}", exc_info=True)
        return False
    finally:
        poll_lock.release()


def run_webhook_script(series_id):
    """Run the webhook script for a single series.

    Runs WITHOUT any in-memory lock so webhooks for different series proceed
    concurrently (bounded by webhook_executor's max_workers). Double-processing of
    the *same* series across the poll/webhook subprocesses is prevented by
    claim_series_for_processing in media_automation.py, not by a scheduler lock.
    """
    try:
        cmd = [sys.executable, "/app/media_automation.py", "webhook", str(series_id)]
        log.info(f"Running: {' '.join(cmd)}")
        result = _run_tracked(cmd, timeout=SCRIPT_TIMEOUT)
        if result.returncode != 0:
            log.error(f"Webhook script exited with code {result.returncode}")
            return False
        return True
    except subprocess.TimeoutExpired:
        log.error(f"Webhook script timed out after {SCRIPT_TIMEOUT}s")
        return False
    except Exception as e:
        log.error(f"Failed to run webhook script: {e}", exc_info=True)
        return False


# Series ids accepted by the webhook and not yet finished. Dedupes bursts of the
# same SeriesAdd and bounds the executor's work queue (see MAX_PENDING_WEBHOOKS).
_pending_lock = threading.Lock()
_pending_series = set()


def enqueue_webhook_series(series_id):
    """Queue a validated series id for processing.

    Returns "queued", "already_queued" (same id still pending — deduplicated) or
    "full" (MAX_PENDING_WEBHOOKS distinct ids already waiting).
    """
    with _pending_lock:
        if series_id in _pending_series:
            return "already_queued"
        if len(_pending_series) >= MAX_PENDING_WEBHOOKS:
            return "full"
        _pending_series.add(series_id)
    try:
        webhook_executor.submit(process_webhook_series, series_id)
    except Exception:
        # Executor shut down (container stopping) — don't leak the pending slot.
        with _pending_lock:
            _pending_series.discard(series_id)
        raise
    return "queued"


def process_webhook_series(series_id):
    """Process a webhook series - runs independently from the poll script."""
    try:
        log.info(f"Webhook: queuing series {series_id} for processing")
        run_webhook_script(series_id)
    finally:
        with _pending_lock:
            _pending_series.discard(series_id)


def _valid_series_id(value):
    """Sonarr series ids are positive ints; reject bools, strings, floats, huge ints."""
    return (isinstance(value, int) and not isinstance(value, bool)
            and 1 <= value <= MAX_SERIES_ID)


def check_webhook_auth(headers, path, secret=None):
    """Authorize a webhook request. Returns None if allowed, else an HTTP status.

    403 when no WEBHOOK_SECRET is configured (webhooks disabled until it is);
    401 when the request carries no / the wrong secret. The secret is accepted as
    the Basic-auth password (or username, if the password field was left empty)
    or as ?token=<secret> in the URL. Constant-time comparison throughout.
    """
    secret = WEBHOOK_SECRET if secret is None else secret
    if not secret:
        return 403
    expected = secret.encode("utf-8")
    candidates = []

    auth = headers.get("Authorization") or ""
    scheme, _, value = auth.partition(" ")
    if scheme.lower() == "basic" and value:
        try:
            decoded = base64.b64decode(value.strip(), validate=True).decode("utf-8")
            user, _, password = decoded.partition(":")
            candidates.extend([password, user])
        except (ValueError, UnicodeDecodeError):
            pass
    elif scheme.lower() == "bearer" and value:
        candidates.append(value.strip())

    query = parse_qs(urlsplit(path or "").query)
    candidates.extend(query.get("token", [])[:1])

    for candidate in candidates:
        if candidate and hmac.compare_digest(candidate.encode("utf-8"), expected):
            return None
    return 401


class BadContentLength(Exception):
    """Content-Length header missing/non-integer/negative → answer with 400."""


class BodyTooLarge(Exception):
    """Content-Length exceeds MAX_WEBHOOK_BODY → answer with 413."""


def read_request_body(headers, rfile, max_bytes=MAX_WEBHOOK_BODY):
    """Parse Content-Length and read exactly that many bytes from rfile.

    Pure helper so it can be unit-tested without a live socket. Raises
    BadContentLength for a malformed/negative length and BodyTooLarge for one
    that exceeds max_bytes — callers translate those into 400 / 413 responses.
    Returns the raw body bytes (possibly empty).
    """
    raw = headers.get('Content-Length', '0')
    try:
        content_length = int(raw)
    except (TypeError, ValueError):
        raise BadContentLength(f"non-integer Content-Length: {raw!r}")
    if content_length < 0:
        raise BadContentLength(f"negative Content-Length: {content_length}")
    if content_length > max_bytes:
        raise BodyTooLarge(f"Content-Length {content_length} exceeds cap {max_bytes}")
    return rfile.read(content_length) if content_length else b""


class _HeaderBudgetReader:
    """Wraps the handler's rfile so readline() — used only for the request line
    and headers — can return at most `budget` bytes in total. Once exhausted it
    returns b"" (http.client then stops reading headers) and sets .overflowed so
    the handler answers 431. read() (the body, already capped by Content-Length
    and MAX_WEBHOOK_BODY) is passed through unchanged."""

    def __init__(self, raw, budget):
        self._raw = raw
        self._remaining = budget
        self.overflowed = False

    def readline(self, size=-1):
        if self._remaining <= 0:
            self.overflowed = True
            return b""
        limit = self._remaining if size is None or size < 0 else min(size, self._remaining)
        line = self._raw.readline(limit)
        self._remaining -= len(line)
        if self._remaining <= 0 and not line.endswith(b"\n"):
            self.overflowed = True
        return line

    def __getattr__(self, name):
        return getattr(self._raw, name)


class WebhookHandler(BaseHTTPRequestHandler):
    """Handle incoming webhooks from Sonarr."""

    # Per-socket-operation timeout (applied by StreamRequestHandler.setup). This
    # resets on every recv, so setup() below also arms a whole-connection
    # deadline — together a slow-loris client can hold a slot for at most
    # WEBHOOK_REQUEST_TIMEOUT seconds.
    timeout = WEBHOOK_REQUEST_TIMEOUT

    def setup(self):
        super().setup()
        self.rfile = _HeaderBudgetReader(self.rfile, MAX_HEADER_BYTES)
        self._deadline_timer = threading.Timer(WEBHOOK_REQUEST_TIMEOUT, self._expire_connection)
        self._deadline_timer.daemon = True
        self._deadline_timer.start()

    def _expire_connection(self):
        """Deadline hit: shut the socket so any blocked read in this handler
        returns immediately and the thread (and its connection slot) is freed."""
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def finish(self):
        timer = getattr(self, "_deadline_timer", None)
        if timer is not None:
            timer.cancel()
        try:
            super().finish()
        except OSError:
            pass  # socket already torn down by the deadline timer

    def _headers_overflowed(self):
        if getattr(getattr(self, "rfile", None), "overflowed", False) is True:
            self._respond(431, {"status": "error", "message": "request headers too large"})
            return True
        return False

    def do_POST(self):
        """Handle POST requests (Sonarr webhooks)."""
        if self._headers_overflowed():
            return
        try:
            # Authorize BEFORE reading the body: unauthenticated clients get no
            # further than the headers (S1/S2 hardening).
            denied = check_webhook_auth(self.headers, getattr(self, "path", "/"))
            if denied == 403:
                _warn_webhook_secret_unset()
                self._respond(403, {"status": "error", "message": "webhooks disabled: WEBHOOK_SECRET not set"})
                return
            if denied == 401:
                log.warning("Webhook rejected: missing or invalid secret")
                self._respond(401, {"status": "error", "message": "unauthorized"},
                              extra_headers={"WWW-Authenticate": 'Basic realm="cascade-media"'})
                return

            # Sonarr always sends application/json. Requiring it also defeats
            # cross-site "simple" text/plain form posts from a browser.
            content_type = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if content_type != "application/json":
                log.warning(f"Webhook rejected: unsupported Content-Type {safe_log(content_type, 60)!r}")
                self._respond(415, {"status": "error", "message": "Content-Type must be application/json"})
                return

            # Parse Content-Length and read the body so a malformed or oversized
            # header returns a clean 400/413 instead of an unhandled traceback.
            try:
                body = read_request_body(self.headers, self.rfile)
            except BadContentLength as e:
                log.warning(f"Webhook rejected: {e}")
                self._respond(400, {"status": "error", "message": "invalid Content-Length"})
                return
            except BodyTooLarge as e:
                log.warning(f"Webhook rejected: {e}")
                self._respond(413, {"status": "error", "message": "request body too large"})
                return

            try:
                data = json.loads(body) if body else {}
            except (ValueError, UnicodeDecodeError):
                log.warning("Webhook rejected: body is not valid JSON")
                self._respond(400, {"status": "error", "message": "invalid JSON"})
                return
            if not isinstance(data, dict):
                self._respond(400, {"status": "error", "message": "JSON body must be an object"})
                return

            event_type = data.get("eventType", "unknown")
            series_data = data.get("series")
            if not isinstance(series_data, dict):
                series_data = {}
            series_title = series_data.get("title", "unknown")
            series_id = series_data.get("id")

            log.info(f"Webhook received: {safe_log(event_type, 40)} for "
                     f"'{safe_log(series_title, 120)}' (ID: {safe_log(series_id, 20)})")

            if event_type == "SeriesAdd":
                if not _valid_series_id(series_id):
                    log.warning(f"Webhook rejected: invalid series id {safe_log(series_id, 20)}")
                    self._respond(400, {"status": "error", "message": "series.id must be a positive integer"})
                    return
                outcome = enqueue_webhook_series(series_id)
                if outcome == "full":
                    log.warning(f"Webhook queue full ({MAX_PENDING_WEBHOOKS} pending); "
                                f"series {series_id} left for the next poll")
                    self._respond(503, {"status": "busy", "seriesId": series_id})
                elif outcome == "already_queued":
                    log.info(f"Series {series_id} already queued; ignoring duplicate webhook")
                    self._respond(200, {"status": "already_queued", "seriesId": series_id})
                else:
                    # Process this series independently from the poll script
                    log.info(f"SeriesAdd detected - scheduling series {series_id}")
                    self._respond(200, {"status": "triggered", "seriesId": series_id})

            elif event_type == "Test":
                self._respond(200, {"status": "ok"})

            elif event_type == "Grab":
                # Grab events don't need immediate processing
                log.info(f"Grab event for '{safe_log(series_title, 120)}', will be handled by next poll")
                self._respond(200, {"status": "noted"})

            else:
                log.info(f"Ignoring event type: {safe_log(event_type, 40)}")
                self._respond(200, {"status": "ignored"})

        except Exception as e:
            log.error(f"Webhook error: {safe_log(e)}", exc_info=True)
            # Generic message: exception text may describe internals.
            self._respond(500, {"status": "error", "message": "internal error"})

    def do_GET(self):
        """Health check endpoint. 503 while the in-process playback loop is
        stalled, so Docker's HEALTHCHECK marks the container unhealthy."""
        if self._headers_overflowed():
            return
        if playback_loop_stalled():
            self._respond(503, {"status": "degraded", "playback_loop": "stalled"})
            return
        self._respond(200, {"status": "running"})

    def _respond(self, code, data, extra_headers=None):
        """Send a JSON response. A client that already hung up (or a connection
        torn down by the deadline) is ignored rather than producing a traceback
        per connection — otherwise a flood of drop-and-run clients floods stderr."""
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            pass

    def log_message(self, format, *args):
        """Suppress default HTTP logging — we use our own."""
        pass


_secret_warning_lock = threading.Lock()
_secret_warning_last = [0.0]


def _warn_webhook_secret_unset():
    """Log the 'webhooks disabled' explanation at most once per 10 minutes."""
    now = time.monotonic()
    with _secret_warning_lock:
        if _secret_warning_last[0] and now - _secret_warning_last[0] < 600:
            return
        _secret_warning_last[0] = now
    log.error("Webhook refused: WEBHOOK_SECRET is not set. Set WEBHOOK_SECRET in the "
              "container env and the same value as the Password of Sonarr's webhook "
              "connection (Settings > Connect > Webhook). Until then new series are "
              "picked up by the regular poll instead.")


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer with a cap on concurrent connections.

    The stock server starts one thread per connection with no limit, so a flood
    of slow or header-heavy connections could exhaust threads and memory. Excess
    connections are closed immediately; slots are released when the handler
    thread finishes (including after a deadline-forced teardown).
    """

    daemon_threads = True

    def __init__(self, server_address, handler_class, max_connections=None):
        self.max_connections = max_connections or MAX_WEBHOOK_CONNECTIONS
        self._conn_slots = threading.BoundedSemaphore(self.max_connections)
        self._last_reject_log = 0.0
        super().__init__(server_address, handler_class)

    def process_request(self, request, client_address):
        if not self._conn_slots.acquire(blocking=False):
            now = time.monotonic()
            if now - self._last_reject_log > 60:
                self._last_reject_log = now
                log.warning(f"Webhook server at its {self.max_connections}-connection cap; "
                            f"dropping new connections")
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._conn_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._conn_slots.release()


def start_webhook_server(server):
    """Run the webhook HTTPServer until its .shutdown() is called from another thread."""
    try:
        log.info(f"Webhook server listening on port {WEBHOOK_PORT}")
        server.serve_forever()
    except Exception as e:
        log.error(f"Webhook server error: {e}", exc_info=True)


def run_trakt_script(args=None):
    """Run the Trakt discovery script."""
    if not trakt_lock.acquire(blocking=False):
        log.info("Trakt discovery already running, skipping")
        return False

    try:
        cmd = [sys.executable, "/app/trakt_discovery.py"]
        if args:
            cmd.extend(args)
        log.info(f"Running: {' '.join(cmd)}")
        result = _run_tracked(cmd, timeout=TRAKT_SCRIPT_TIMEOUT)
        if result.returncode != 0:
            log.error(f"Trakt script exited with code {result.returncode}")
            return False
        return True
    except subprocess.TimeoutExpired:
        log.error(f"Trakt script timed out after {TRAKT_SCRIPT_TIMEOUT}s")
        return False
    except Exception as e:
        log.error(f"Failed to run Trakt script: {e}", exc_info=True)
        return False
    finally:
        trakt_lock.release()


# Playback-loop watchdog state. The in-process loop has no subprocess hard
# timeout, and requests' timeout=30 is per-read, so a slow-drip upstream response
# can block the loop thread indefinitely. The loop stamps a heartbeat after each
# iteration; playback_watchdog_loop() restarts a stalled loop under a new
# generation number (the stale thread exits if it ever wakes) and do_GET reports
# 503 while it is stalled so the container shows unhealthy.
PLAYBACK_STALL_SECONDS = max(10 * PLAYBACK_CHECK_INTERVAL, 15 * 60)
_playback_state_lock = threading.Lock()
_playback_state = {"heartbeat": None, "generation": 0, "disabled": False}


def _playback_heartbeat():
    with _playback_state_lock:
        _playback_state["heartbeat"] = time.monotonic()


def playback_loop_stalled(now=None):
    """True if the playback loop is running but hasn't finished an iteration
    within PLAYBACK_STALL_SECONDS."""
    now = time.monotonic() if now is None else now
    with _playback_state_lock:
        hb = _playback_state["heartbeat"]
        if hb is None or _playback_state["disabled"]:
            return False
        return now - hb > PLAYBACK_STALL_SECONDS


def _start_playback_thread():
    """Start a fresh playback loop under a new generation; returns the thread."""
    with _playback_state_lock:
        _playback_state["generation"] += 1
        generation = _playback_state["generation"]
        _playback_state["heartbeat"] = time.monotonic()
    thread = threading.Thread(target=playback_check_loop, args=(generation,),
                              daemon=True, name=f"playback-{generation}")
    thread.start()
    return thread


def playback_watchdog_loop(check_every=60):
    """Restart the playback loop if it stalls (S40)."""
    while not shutdown_event.wait(check_every):
        if playback_loop_stalled():
            with _playback_state_lock:
                stale_for = time.monotonic() - (_playback_state["heartbeat"] or 0)
            log.error(f"Playback check loop stalled for {stale_for:.0f}s "
                      f"(limit {PLAYBACK_STALL_SECONDS}s) — likely a hung upstream "
                      f"request. Starting a fresh loop.")
            _start_playback_thread()


def playback_check_loop(generation=None):
    """Background loop that checks for active playback every N seconds — IN-PROCESS.

    Previously each interval shelled out to a fresh `python media_automation.py
    playback` subprocess (~1900 cold interpreter starts/day at the default 45s).
    We now run the check in-process, reusing ONE long-lived sqlite3 connection and
    ONE requests.Session (keep-alive) across iterations.

    Safety design (these resources are confined to THIS single loop thread):
      - conn + session are created once here and used ONLY by this thread, so we
        sidestep sqlite/requests cross-thread concerns. The poll/webhook paths
        keep using their own subprocesses with their own connections.
      - shutdown_event is passed to check_active_playback as stop_event, so the
        long SABnzbd-queue wait (~120s) and the boost retry-backoffs become
        interruptible: a SIGTERM returns from the wait immediately instead of
        freezing this thread / delaying container shutdown.
      - The per-iteration call is wrapped in try/except so any playback failure
        is logged and the loop continues — it must NEVER crash the scheduler.
      - The old 600s subprocess hard-timeout is gone, and timeout=30 is only a
        per-read timeout, so a slow-drip upstream can still block a call. Each
        iteration stamps a heartbeat; playback_watchdog_loop() replaces a stalled
        loop (new `generation`) and this thread exits if it ever wakes up stale.
    """
    if generation is None:
        with _playback_state_lock:
            generation = _playback_state["generation"]

    def _current():
        with _playback_state_lock:
            return _playback_state["generation"] == generation

    log.info(f"Playback check loop started (interval: {PLAYBACK_CHECK_INTERVAL}s, in-process)")

    # Long-lived resources owned solely by this loop thread.
    conn = None
    session = None
    try:
        conn = media_automation.init_db()
        if not media_automation.check_db_writable(conn):
            log.error("Playback loop: DB not writable; playback checks disabled this run")
            with _playback_state_lock:
                _playback_state["disabled"] = True  # don't let the watchdog respawn it
            return  # finally closes conn
        session = requests.Session()

        while not shutdown_event.is_set() and _current():
            try:
                media_automation.check_active_playback(
                    conn, session=session, stop_event=shutdown_event
                )
            except Exception as e:
                # Error boundary: a playback failure must never crash the scheduler.
                log.error(f"Error in playback check loop: {e}", exc_info=True)
            if not _current():
                log.warning("Stale playback loop woke up after being replaced; exiting")
                break
            _playback_heartbeat()
            # wait() returns True if event was set during the wait — break promptly on SIGTERM
            if shutdown_event.wait(PLAYBACK_CHECK_INTERVAL):
                break
    except Exception as e:
        # Setup (init_db / Session) failed — log and exit the thread cleanly rather
        # than letting an unhandled exception kill it silently.
        log.error(f"Playback loop failed to start: {e}", exc_info=True)
    finally:
        # Close the long-lived resources on shutdown (or setup failure).
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        log.info("Playback check loop stopped")


def trakt_discovery_loop():
    """Background loop that runs Trakt discovery at a scheduled daily clock time (UTC)."""
    log.info(f"Trakt discovery loop started (daily at {TRAKT_DISCOVERY_TIME} {TRAKT_DISCOVERY_TZ_NAME})")
    while not shutdown_event.is_set():
        next_run = next_discovery_run()
        wait = (next_run - datetime.now(TRAKT_DISCOVERY_TZ)).total_seconds()
        log.info(f"Trakt discovery next run: {next_run.strftime('%Y-%m-%d %H:%M')} {TRAKT_DISCOVERY_TZ_NAME} ({wait/3600:.1f}h from now)")
        if shutdown_event.wait(wait):
            break
        try:
            run_trakt_script(["discover"])
        except Exception as e:
            log.error(f"Error in Trakt discovery loop: {e}", exc_info=True)


def _handle_shutdown_signal(signum, frame):
    """SIGTERM/SIGINT handler — sets the shutdown_event so all loops can exit cleanly,
    and forwards a terminate() to any live child subprocess.

    Why this is safe to do from a signal handler: Python only runs signal handlers
    between bytecode instructions on the main thread, never re-entrantly mid-opcode.
    The one deadlock risk would be the handler firing while the main thread already
    holds _children_lock (inside _run_tracked's brief add/discard window). We avoid
    that by acquiring the lock non-blocking: if we can't get it, we fall back to a
    best-effort snapshot. The lock is only ever held for a couple of bytecodes
    (set.add / set.discard), never across proc.wait(), so cross-thread contention
    here is negligible. We do the minimum: snapshot the live children, then
    terminate() each (a single SIGTERM via kill(2)) — no heavy/reentrant work. The
    blocked run_* helper's proc.wait() then returns promptly, the child exits cleanly
    inside stop_grace_period instead of being SIGKILLed mid-write, and shutdown_event
    lets the loops stop.
    """
    log.info(f"Received signal {signum}, beginning graceful shutdown")
    shutdown_event.set()
    if _children_lock.acquire(blocking=False):
        try:
            children = list(_live_children)
        finally:
            _children_lock.release()
    else:
        # Couldn't grab the lock (we likely interrupted our own add/discard). Take a
        # best-effort copy; tuple() over a set is a single C-level op and the set is
        # only mutated under the lock, so this is safe enough for a one-shot signal.
        children = list(tuple(_live_children))
    for proc in children:
        try:
            proc.terminate()
        except Exception:
            pass


def main():
    # Files we create (SQLite DB + WAL, Trakt tokens, AI artifacts) must not be
    # world-readable; children inherit this umask. Group stays rw for Unraid's
    # 'users' group (S25).
    os.umask(0o007)
    log.info(f"Media Automation Scheduler started")
    log.info(f"  Polling interval: {INTERVAL_MINUTES} minutes")
    log.info(f"  Webhook port: {WEBHOOK_PORT} (auth: {'secret required' if WEBHOOK_SECRET else 'DISABLED - WEBHOOK_SECRET unset'})")
    log.info(f"  Playback check interval: {PLAYBACK_CHECK_INTERVAL}s")
    log.info(f"  Script timeout: {SCRIPT_TIMEOUT}s")
    log.info(f"  Trakt discovery: {'enabled' if TRAKT_DISCOVERY_ENABLED else 'disabled'}")
    if TRAKT_DISCOVERY_ENABLED:
        next_run = next_discovery_run()
        wait = (next_run - datetime.now(timezone.utc)).total_seconds()
        log.info(f"  Trakt discovery schedule: daily at {TRAKT_DISCOVERY_TIME} {TRAKT_DISCOVERY_TZ_NAME}")
        log.info(f"  Trakt next run: {next_run.strftime('%Y-%m-%d %H:%M')} {TRAKT_DISCOVERY_TZ_NAME} ({wait/3600:.1f}h from now)")

    # Register graceful-shutdown handlers (must be done in main thread)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    signal.signal(signal.SIGINT, _handle_shutdown_signal)

    # Build webhook server up front so we hold a reference for .shutdown().
    # BoundedThreadingHTTPServer handles each request in its own (daemon) thread,
    # so one slow client can't block health checks or other webhooks, while the
    # connection cap + per-connection deadline stop a flood from exhausting
    # threads or memory.
    if not WEBHOOK_SECRET:
        log.error("WEBHOOK_SECRET is not set: Sonarr webhooks will be REFUSED (403). "
                  "Set WEBHOOK_SECRET and put the same value in the Password field of "
                  "Sonarr's webhook connection. New series are still picked up by the "
                  f"{INTERVAL_MINUTES}-minute poll.")

    webhook_server = None
    try:
        webhook_server = BoundedThreadingHTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
    except OSError as e:
        log.error(f"Failed to start webhook server on port {WEBHOOK_PORT}: {e}")
        log.error("Continuing with polling-only mode")

    if webhook_server is not None:
        webhook_thread = threading.Thread(
            target=start_webhook_server, args=(webhook_server,), daemon=True
        )
        webhook_thread.start()

    # Start playback check loop in background, plus its stall watchdog
    _start_playback_thread()
    threading.Thread(target=playback_watchdog_loop, daemon=True, name="playback-watchdog").start()

    # Start Trakt discovery loop if enabled
    if TRAKT_DISCOVERY_ENABLED:
        trakt_thread = threading.Thread(target=trakt_discovery_loop, daemon=True)
        trakt_thread.start()

    # Run catch-up on first start
    if RUN_CATCHUP_ON_START:
        log.info("Running initial catch-up for existing series...")
        run_poll_script(["catchup"])

    # Main polling loop (backup for webhooks)
    while not shutdown_event.is_set():
        try:
            run_poll_script()
            if shutdown_event.is_set():
                break
            log.info(f"Sleeping for {INTERVAL_MINUTES} minutes...")
            if shutdown_event.wait(INTERVAL_MINUTES * 60):
                break
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
            if shutdown_event.wait(60):
                break

    log.info("Shutting down...")
    if webhook_server is not None:
        webhook_server.shutdown()  # unblocks serve_forever() in the webhook thread
    # cancel_futures discards pending webhook tasks; running ones complete or die on SIGKILL
    webhook_executor.shutdown(wait=False, cancel_futures=True)
    log.info("Shutdown complete")


if __name__ == "__main__":
    main()
