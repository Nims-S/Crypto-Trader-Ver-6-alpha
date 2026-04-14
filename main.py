from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
import os

from bot import run_bot
from db import init_db, get_conn
from config import PORT, SYMBOLS, CAPITAL, BOT_VERSION, RESET_TOKEN
from price_feed import feeds
from risk import get_dynamic_capital
from state import get_state


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

def get_positions_from_db():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT symbol, entry, sl, tp, size, regime, confidence FROM positions")
        rows = cur.fetchall()

        return [{
            "symbol": r[0],
            "entry": r[1],
            "sl": r[2],
            "tp": r[3],
            "size": r[4],
            "regime": r[5],
            "confidence": r[6]
        } for r in rows]

    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()

@app.route("/")
def home():
    return jsonify({"status": "running", "engine": f"algobot_{BOT_VERSION}"})

@app.route("/positions")
def positions_view():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT symbol, entry, sl, tp, size, regime, confidence FROM positions")
        rows = cur.fetchall()

        formatted = []
        for r in rows:
            symbol = r[0]
            feed = feeds.get(symbol)
            try:
                live_price = feed.get_price() if feed else None
            except Exception:
                live_price = None
            display_price = live_price if live_price else r[1]
            pnl = round((display_price - r[1]) * r[4], 2)

            formatted.append({
                "symbol": symbol,
                "entry": r[1],
                "sl": r[2],
                "tp": r[3],
                "size": r[4],
                "regime": r[5],
                "confidence": r[6],
                "current_price": display_price,
                "pnl": pnl,
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
    return jsonify({
        "state": get_state(),
        "positions": get_positions_from_db()
    })

@app.route("/risk")
def risk_report():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        total_cap = get_dynamic_capital(cur, CAPITAL)
        cur.execute("SELECT symbol, entry, size FROM positions")
        positions = cur.fetchall()

        deployed_capital = sum(p[1] * p[2] for p in positions)
        available_capital = total_cap - deployed_capital

        breakdown = {}
        for p in positions:
            sym = p[0].split('/')[0]
            val = p[1] * p[2]
            breakdown[sym] = round((val / total_cap) * 100, 2) if total_cap > 0 else 0

        return jsonify({
            "version": BOT_VERSION,
            "total_capital": round(total_cap, 2),
            "deployed_capital": round(deployed_capital, 2),
            "available_capital": round(available_capital, 2),
            "ratios": breakdown,
            "allocation_pct": round((deployed_capital / total_cap) * 100, 2) if total_cap > 0 else 0
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route("/status")
def status():
    return jsonify({
        "status": "Healthy",
        "botStatus": "running",
        "version": BOT_VERSION,
        "server_time": time.time()
    })

@app.route("/trades")
def trades():
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT symbol, entry, exit, pnl, regime, reason, confidence, timestamp FROM trades ORDER BY timestamp DESC LIMIT 10")
        rows = cur.fetchall()
        return jsonify([{
            "symbol": r[0],
            "entry": r[1],
            "exit": r[2],
            "pnl": r[3],
            "regime": r[4],
            "reason": r[5],
            "confidence": r[6],
            "timestamp": r[7]
        } for r in rows])
    except Exception:
        return jsonify([])
    finally:
        if conn:
            conn.close()
@app.route("/caffeine/health", methods=["GET"])
def caffeine_health():
    state = get_state()
    return jsonify({
        "status": "ok",
        "last_update": state.get("last_update"),
        "assets_tracked": len(state.get("assets", {}))
    })
@app.route("/debug")
def debug():
    return {"bot": "running", "version": BOT_VERSION}

@app.route("/health")
def health():
    return jsonify({"status": "alive", "version": BOT_VERSION, "server_time": time.time()})

@app.route("/reset", methods=["POST"])
def reset():
    if RESET_TOKEN:
        token = request.args.get("token") or request.headers.get("X-Reset-Token")
        if token != RESET_TOKEN:
            return jsonify({"error": "unauthorized"}), 403

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM positions")
        cur.execute("DELETE FROM trades")
        conn.commit()
        return jsonify({"status": "reset done"})
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

def background_executor():
    print("⏳ Waiting for web server to stabilize...", flush=True)
    time.sleep(5)

    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("SELECT pg_try_advisory_lock(99999)")
        if not cur.fetchone()[0]:
            print("⚠️ Another bot instance is already running. Skipping start.", flush=True)
            return

        print("🗄️ Initializing Database...", flush=True)
        init_db()

        print("🤖 Starting Bot Loop...", flush=True)
        run_bot()

    except Exception as e:
        print(f"❌ CRITICAL BACKGROUND ERROR: {e}", flush=True)

    finally:
        if conn:
            conn.close()

t = threading.Thread(target=background_executor, daemon=True)
t.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", PORT))
    app.run(host="0.0.0.0", port=port)
