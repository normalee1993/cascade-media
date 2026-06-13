#!/usr/bin/env python3
"""Scheduler with webhook support for media automation."""

import time
import os
import logging
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
import json

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
RUN_CATCHUP_ON_START = os.getenv("RUN_CATCHUP_ON_START", "true").lower() == "true"
WEBHOOK_PORT = get_int_env("WEBHOOK_PORT", 9191)
SCRIPT_TIMEOUT = get_int_env("SCRIPT_TIMEOUT_MINUTES", 30) * 60
PLAYBACK_CHECK_INTERVAL = get_int_env("PLAYBACK_CHECK_INTERVAL", 45)
PLAYBACK_SCRIPT_TIMEOUT = get_int_env("PLAYBACK_SCRIPT_TIMEOUT", 600)

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


# Separate locks for polling vs webhook vs playback vs trakt processing
poll_lock = threading.Lock()
webhook_lock = threading.Lock()
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
# Per-request socket timeout so a half-open / slow-loris connection can't pin a
# handler thread forever.
WEBHOOK_REQUEST_TIMEOUT = 30

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


def run_playback_script():
    """Run the playback check script."""
    if not playback_lock.acquire(blocking=False):
        log.debug("Playback check already running, skipping")
        return False

    try:
        cmd = [sys.executable, "/app/media_automation.py", "playback"]
        log.debug(f"Running: {' '.join(cmd)}")
        result = _run_tracked(cmd, timeout=PLAYBACK_SCRIPT_TIMEOUT)
        if result.returncode != 0:
            log.error(f"Playback script exited with code {result.returncode}")
            return False
        return True
    except subprocess.TimeoutExpired:
        log.error(f"Playback script timed out after {PLAYBACK_SCRIPT_TIMEOUT}s")
        return False
    except Exception as e:
        log.error(f"Failed to run playback script: {e}", exc_info=True)
        return False
    finally:
        playback_lock.release()


def run_webhook_script(series_id):
    """Run the webhook script for a single series. Waits if another webhook is processing."""
    # Block and wait (don't skip) - every webhook series must be processed
    with webhook_lock:
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


def process_webhook_series(series_id):
    """Process a webhook series - runs independently from the poll script."""
    log.info(f"Webhook: queuing series {series_id} for processing")
    run_webhook_script(series_id)


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


class WebhookHandler(BaseHTTPRequestHandler):
    """Handle incoming webhooks from Sonarr."""

    # Per-request socket timeout (used by BaseHTTPRequestHandler.handle / setup).
    # A half-open or slow client gets its socket torn down instead of pinning a
    # handler thread indefinitely.
    timeout = WEBHOOK_REQUEST_TIMEOUT

    def do_POST(self):
        """Handle POST requests (Sonarr webhooks)."""
        # Parse Content-Length and read the body INSIDE the try so a malformed or
        # oversized header returns a clean 400/413 instead of an unhandled traceback.
        try:
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

            data = json.loads(body) if body else {}
            event_type = data.get("eventType", "unknown")
            series_data = data.get("series", {})
            series_title = series_data.get("title", "unknown")
            series_id = series_data.get("id")

            log.info(f"Webhook received: {event_type} for '{series_title}' (ID: {series_id})")

            if event_type == "SeriesAdd" and series_id:
                # Process this series independently from the poll script
                log.info(f"SeriesAdd detected - scheduling series {series_id}")
                webhook_executor.submit(process_webhook_series, series_id)
                self._respond(200, {"status": "triggered", "seriesId": series_id})

            elif event_type == "Test":
                self._respond(200, {"status": "ok"})

            elif event_type == "Grab":
                # Grab events don't need immediate processing
                log.info(f"Grab event for '{series_title}', will be handled by next poll")
                self._respond(200, {"status": "noted"})

            else:
                log.info(f"Ignoring event type: {event_type}")
                self._respond(200, {"status": "ignored"})

        except Exception as e:
            log.error(f"Webhook error: {e}", exc_info=True)
            self._respond(500, {"status": "error", "message": str(e)})

    def do_GET(self):
        """Health check endpoint."""
        self._respond(200, {"status": "running"})

    def _respond(self, code, data):
        """Send a JSON response."""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        """Suppress default HTTP logging — we use our own."""
        pass


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


def playback_check_loop():
    """Background loop that checks for active playback every N seconds.

    TODO (P2-10, deferred): each iteration shells out to a fresh
    `python /app/media_automation.py playback` subprocess via run_playback_script().
    With the default PLAYBACK_CHECK_INTERVAL=45s that is ~1900 cold Python
    interpreter starts/day, each of which re-imports the module and re-opens the
    SQLite DB and a new HTTP connection to Jellyfin/Sonarr just to poll for active
    sessions — pure overhead for a check that is usually a no-op.

    Recommended future direction: run the playback poll in-process here, reusing a
    single long-lived sqlite3 connection and one requests.Session (keep-alive) across
    iterations instead of spawning a subprocess. That work is intentionally NOT done
    in this change because the playback logic lives in media_automation.py (owned by
    a sibling change) and refactoring it for safe in-process reuse — connection
    lifecycle, thread-safety with the webhook/poll paths, DRY_RUN handling — is the
    riskiest part. Keep the subprocess model until that migration is scoped.
    """
    log.info(f"Playback check loop started (interval: {PLAYBACK_CHECK_INTERVAL}s)")
    while not shutdown_event.is_set():
        try:
            run_playback_script()
        except Exception as e:
            log.error(f"Error in playback check loop: {e}", exc_info=True)
        # wait() returns True if event was set during the wait — break promptly on SIGTERM
        if shutdown_event.wait(PLAYBACK_CHECK_INTERVAL):
            break


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
    log.info(f"Media Automation Scheduler started")
    log.info(f"  Polling interval: {INTERVAL_MINUTES} minutes")
    log.info(f"  Webhook port: {WEBHOOK_PORT}")
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
    # ThreadingHTTPServer handles each request in its own thread, so one slow or
    # stuck client can't block health checks (do_GET) or other webhooks. The worker
    # threads are daemons (daemon_threads=True) so a pending request can't block a
    # clean process exit.
    webhook_server = None
    try:
        webhook_server = ThreadingHTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
        webhook_server.daemon_threads = True
    except OSError as e:
        log.error(f"Failed to start webhook server on port {WEBHOOK_PORT}: {e}")
        log.error("Continuing with polling-only mode")

    if webhook_server is not None:
        webhook_thread = threading.Thread(
            target=start_webhook_server, args=(webhook_server,), daemon=True
        )
        webhook_thread.start()

    # Start playback check loop in background
    playback_thread = threading.Thread(target=playback_check_loop, daemon=True)
    playback_thread.start()

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
