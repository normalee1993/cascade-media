#!/usr/bin/env python3
"""Scheduler with webhook support for media automation."""

import time
import os
import logging
import subprocess
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from collections import deque
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

# Separate locks for polling vs webhook vs playback processing
poll_lock = threading.Lock()
webhook_lock = threading.Lock()
playback_lock = threading.Lock()

# Queue for webhook series that arrive while another webhook is processing
pending_series = deque()
pending_lock = threading.Lock()

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


def start_webhook_server():
    """Start the webhook listener in a background thread."""
    try:
        server = HTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler)
        log.info(f"Webhook server listening on port {WEBHOOK_PORT}")
        server.serve_forever()
    except OSError as e:
        log.error(f"Failed to start webhook server on port {WEBHOOK_PORT}: {e}")
        log.error("Continuing with polling-only mode")
    except Exception as e:
        log.error(f"Webhook server error: {e}", exc_info=True)


def playback_check_loop():
    """Background loop that checks for active playback every N seconds."""
    log.info(f"Playback check loop started (interval: {PLAYBACK_CHECK_INTERVAL}s)")
    while True:
        try:
            run_playback_script()
        except Exception as e:
            log.error(f"Error in playback check loop: {e}", exc_info=True)
        time.sleep(PLAYBACK_CHECK_INTERVAL)


def main():
    log.info(f"Media Automation Scheduler started")
    log.info(f"  Polling interval: {INTERVAL_MINUTES} minutes")
    log.info(f"  Webhook port: {WEBHOOK_PORT}")
    log.info(f"  Playback check interval: {PLAYBACK_CHECK_INTERVAL}s")
    log.info(f"  Script timeout: {SCRIPT_TIMEOUT}s")

    # Start webhook server in background
    webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
    webhook_thread.start()

    # Start playback check loop in background
    playback_thread = threading.Thread(target=playback_check_loop, daemon=True)
    playback_thread.start()

    # Run catch-up on first start
    if RUN_CATCHUP_ON_START:
        log.info("Running initial catch-up for existing series...")
        run_poll_script(["catchup"])

    # Main polling loop (backup for webhooks)
    while True:
        try:
            run_poll_script()
            log.info(f"Sleeping for {INTERVAL_MINUTES} minutes...")
            time.sleep(INTERVAL_MINUTES * 60)
        except KeyboardInterrupt:
            log.info("Shutting down...")
            webhook_executor.shutdown(wait=False)
            break
        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(60)


if __name__ == "__main__":
    main()
