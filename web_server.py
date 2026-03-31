#!/usr/bin/env python3
"""
Minimal Flask server that exposes /sync endpoint to trigger Airtable comments sync.
Deployed as a free Render web service, triggered daily by an external cron pinger.
"""

import logging
import os
import sys
import threading
from flask import Flask, jsonify

# Force all output to flush immediately (critical for Render logs)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Track sync state
_sync_lock = threading.Lock()
_sync_running = False


@app.route("/")
def health():
    return jsonify({"status": "ok"})


@app.route("/sync", methods=["GET", "POST"])
def sync():
    global _sync_running
    if _sync_running:
        logger.info("Sync already running, returning 409")
        return jsonify({"status": "already_running"}), 409

    def run_sync():
        global _sync_running
        try:
            logger.info("=== SYNC STARTED ===")
            from sync_airtable_comments import main
            main()
            logger.info("=== SYNC COMPLETED ===")
        except Exception:
            logger.exception("=== SYNC FAILED ===")
        finally:
            with _sync_lock:
                _sync_running = False

    with _sync_lock:
        _sync_running = True

    thread = threading.Thread(target=run_sync, daemon=True)
    thread.start()

    logger.info("Sync triggered, running in background")
    return jsonify({"status": "started"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port)
