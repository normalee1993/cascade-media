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
from http.server import HTTPServer, BaseHTTPRequestHandler
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

# Thread pool for webhook handling
webhook_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="webhook")


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
        result = subprocess.run(cmd, capture_output=False, text=True, timeout=SCRIPT_TIMEOUT)
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
        result = subprocess.run(cmd, capture_output=False, text=True, timeout=PLAYBACK_SCRIPT_TIMEOUT)
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
            result = subprocess.run(cmd, capture_output=False, text=True, timeout=SCRIPT_TIMEOUT)
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


class WebhookHandler(BaseHTTPRequestHandler):
    """Handle incoming webhooks from Sonarr."""

    def do_POST(self):
        """Handle POST requests (Sonarr webhooks)."""
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        try:
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
        result = subprocess.run(cmd, capture_output=False, text=True, timeout=TRAKT_SCRIPT_TIMEOUT)
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
    """Background loop that checks for active playback every N seconds."""
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
    """SIGTERM/SIGINT handler — sets the shutdown_event so all loops can exit cleanly."""
    log.info(f"Received signal {signum}, beginning graceful shutdown")
    shutdown_event.set()


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

    # Build webhook HTTPServer up front so we hold a reference for .shutdown()
    webhook_server = None
    try:
        webhook_server = HTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
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
