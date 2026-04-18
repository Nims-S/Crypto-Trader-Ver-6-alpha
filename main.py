"""
main.py — Flask web server + bot thread launcher.

Key fixes vs. previous version:
  - Advisory lock connection is held for the entire bot lifetime (not closed
    in a finally block, which released the lock immediately before).
  - CORS origin regex is anchored so it cannot be bypassed.
  - /reset endpoint now also resets strategy_stats to keep win-rate data
    consistent with the trade history.
  - RESET_TOKEN is enforced: if set, requests without it are rejected.
"""

import os
import re
import threading
import time
import logging

from flask import Flask, jsonify, request
from flask_cors import CORS

from bot import run_bot
from config import PORT, BOT_VERSION, RESET_TOKEN
from db import get_conn, init_db
from price_feed import feeds
from risk import get_dynamic_capital
from state import get_controls, get_state, set_control

app = Flask(__name__)

# ── CORS ──────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS",
        "https://miner-bot-epc.caffeine.xyz,https://caffeine.ai",
    ).split(",")
    if o.strip()
]
# Anchored regex: only subdomains of caffeine.xyz, no bypass via prefix tricks
CAFFEINE_ORIGIN_REGEX = re.compile(r"^https://[a-zA-Z0-9-]+\.caffeine\.xyz$")

CORS(
    app,
    resources={
        r"/caffeine/*": {"origins": ALLOWED_ORIGINS + [CAFFEINE_ORIGIN_REGEX]},
        r"/*":          {"origins": ALLOWED_ORIGINS},
    },
    supports_credentials=False,
)

# ── logging filter ────────────────────────────────────────────────────────────
class IgnoreCaffeineFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "/caffeine/" in msg:
            return "UptimeRobot" in msg
        return True

logging.getLogger("werkzeug").addFilter(IgnoreCaffeineFilter())

# ── bot thread ────────────────────────────────────────────────────────────────
BOT_THREAD_LOCK    = threading.Lock()
BOT_THREAD_STARTED = False
BOT_THREAD_ENABLED = os.getenv("BOT_THREAD_ENABLED", "true").strip().lower() in {
    "1", "true", "yes", "on"
}

# This connection is never closed — it holds the advisory lock for the process
# lifetime so a second Render dyno cannot acquire the same lock and run a
# parallel bot.
_LOCK_CONN = None


def background_executor():
    global _LOCK_CONN
    print("⏳ Waiting for web server to stabilise...", flush=True)
    time.sleep(5)

    try:
        _LOCK_CONN = get_conn()
        cur = _LOCK_CONN.cursor()
        cur.execute("SELECT pg_try_advisory_lock(12345)")
        if not cur.fetchone()[0]:
            print("⚠️  Another bot instance is already running. Skipping start.", flush=True)
            _LOCK_CONN.close()
            _LOCK_CONN = None
            return

        print("🗄️  Initialising database...", flush=True)
        init_db()
        print("🤖 Starting bot loop...", flush=True)
        run_bot()   # blocks until the process exits; lock is held throughout

    except Exception as e:
        print(f"❌ CRITICAL BACKGROUND ERROR: {e}", flush=True)
        # Do NOT close _LOCK_CONN here — that would release the advisory lock.
        # Let the process die and Render restart it.


def start_background_executor_once():
    global BOT_THREAD_STARTED
    if not BOT_THREAD_ENABLED:
        return
    with BOT_THREAD_LOCK:
        if BOT_THREAD_STARTED:
            return
        thread = threading.Thread(target=background_executor, daemon=True)
        thread.start()
        BOT_THREAD_STARTED = True


# ── helpers ───────────────────────────────────────────────────────────────────

def get_positions_from_db():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT symbol, entry, sl, tp, size, regime, confidence FROM positions"
        )
        return [
            {
                "symbol":     r[0], "entry": r[1], "sl": r[2],
                "tp":         r[3], "size":  r[4], "regime": r[5],
                "confidence": r[6],
            }
            for r in cur.fetchall()
        ]
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()


# ── routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    return jsonify({"status": "running", "engine": f"algobot_{BOT_VERSION}"})


@app.route("/positions")
def positions_view():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(
            "SELECT symbol, entry, sl, tp, size, regime, confidence FROM positions"
        )
        rows = cur.fetchall()
        formatted = []
        for r in rows:
            symbol = r[0]
            feed   = feeds.get(symbol)
            try:
                live_price = feed.get_price() if feed else None
            except Exception:
                live_price = None
            display_price = live_price if live_price else r[1]
            pnl = round((display_price - r[1]) * r[4], 2)
            formatted.append({
                "symbol":        symbol,
                "entry":         r[1],
                "sl":            r[2],
                "tp":            r[3],
                "size":          r[4],
                "regime":        r[5],
                "confidence":    r[6],
                "current_price": display_price,
                "pnl":           pnl,
            })
        return jsonify(formatted)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/caffeine/state", methods=["GET"])
def caffeine_state():
    return jsonify(get_state())


@app.route("/caffeine/full", methods=["GET"])
def caffeine_full():
    return jsonify({"state": get_state(), "positions": get_positions_from_db()})


@app.route("/caffeine/controls", methods=["GET"])
def caffeine_controls():
    return jsonify(get_controls())


@app.route("/caffeine/controls", methods=["POST"])
def caffeine_controls_update():
    data               = request.get_json(force=True) or {}
    scope              = data.get("scope", "GLOBAL")
    enabled            = data.get("enabled")
    flatten_on_disable = data.get("flatten_on_disable")
    if enabled is None and flatten_on_disable is None:
        return jsonify({"error": "nothing to update"}), 400
    set_control(scope=scope, enabled=enabled, flatten_on_disable=flatten_on_disable)
    return jsonify({"ok": True, "scope": scope})


@app.route("/risk")
def risk_report():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        total_cap = get_dynamic_capital(cur, 100000)
        cur.execute("SELECT symbol, entry, size FROM positions")
        positions        = cur.fetchall()
        deployed_capital = sum(p[1] * p[2] for p in positions)
        available_capital = total_cap - deployed_capital
        breakdown = {}
        for p in positions:
            sym = p[0].split("/")[0]
            val = p[1] * p[2]
            breakdown[sym] = round((val / total_cap) * 100, 2) if total_cap > 0 else 0
        return jsonify({
            "version":          BOT_VERSION,
            "total_capital":    round(total_cap, 2),
            "deployed_capital": round(deployed_capital, 2),
            "available_capital": round(available_capital, 2),
            "ratios":           breakdown,
            "allocation_pct":   round((deployed_capital / total_cap) * 100, 2) if total_cap > 0 else 0,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/status")
def status():
    return jsonify({
        "status":      "Healthy",
        "botStatus":   "running",
        "version":     BOT_VERSION,
        "server_time": time.time(),
    })


@app.route("/trades")
def trades():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT symbol, entry, exit, pnl, regime, reason, confidence, timestamp
            FROM trades ORDER BY timestamp DESC LIMIT 10
        """)
        return jsonify([
            {
                "symbol":     r[0], "entry":     r[1], "exit": r[2],
                "pnl":        r[3], "regime":    r[4], "reason": r[5],
                "confidence": r[6], "timestamp": r[7],
            }
            for r in cur.fetchall()
        ])
    except Exception:
        return jsonify([])
    finally:
        if conn:
            conn.close()


@app.route("/caffeine/health", methods=["GET"])
def caffeine_health():
    state = get_state()
    return jsonify({
        "status":        "ok",
        "last_update":   state.get("last_update"),
        "assets_tracked": len(state.get("assets", {})),
    })


@app.route("/debug")
def debug():
    return {"bot": "running", "version": BOT_VERSION}


@app.route("/health")
def health():
    return jsonify({"status": "alive", "version": BOT_VERSION, "server_time": time.time()})


@app.route("/reset", methods=["POST"])
def reset():
    # Always require a token when RESET_TOKEN is configured
    if RESET_TOKEN:
        token = request.args.get("token") or request.headers.get("X-Reset-Token")
        if token != RESET_TOKEN:
            return jsonify({"error": "unauthorized"}), 403

    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        # Reset trades AND strategy_stats together so win-rate data stays consistent
        cur.execute("DELETE FROM positions")
        cur.execute("DELETE FROM trades")
        cur.execute("DELETE FROM strategy_stats")
        conn.commit()
        return jsonify({"status": "reset done — positions, trades, and strategy_stats cleared"})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    start_background_executor_once()
    port = int(os.environ.get("PORT", PORT))
    app.run(host="0.0.0.0", port=port)
else:
    start_background_executor_once()