# state.py (NEW FILE)

from datetime import datetime

STATE = {
    "last_update": None,
    "assets": {}
}

def update_asset(symbol, regime, strategy, signal=None, position=None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO asset_state (symbol, regime, strategy, signal, position, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (symbol) DO UPDATE SET
            regime = EXCLUDED.regime,
            strategy = EXCLUDED.strategy,
            signal = EXCLUDED.signal,
            position = EXCLUDED.position,
            updated_at = NOW()
    """, (
        symbol,
        regime,
        strategy,
        json.dumps(signal),
        json.dumps(position)
    ))

    conn.commit()
    conn.close()
    print(f"[STATE UPDATE] {symbol} | regime={regime} | strategy={strategy}", flush=True)
def get_state():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT symbol, regime, strategy, signal, position, updated_at FROM asset_state")

    rows = cur.fetchall()

    assets = {}
    for r in rows:
        assets[r[0]] = {
            "regime": r[1],
            "strategy": r[2],
            "signal": r[3],
            "position": r[4],
            "timestamp": r[5].isoformat()
        }

    return {
        "assets": assets,
        "last_update": datetime.utcnow().isoformat()
    }