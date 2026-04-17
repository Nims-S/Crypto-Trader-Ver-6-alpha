# state.py (NEW FILE)
from datetime import datetime
import json
from db import get_conn

print("✅ STATE.PY LOADED", flush=True)
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
        json.dumps(signal, default=str) if signal else None,
        json.dumps(position, default=str) if position else None
    ))
    print(f"[STATE UPDATE] {symbol} | regime={regime} | strategy={strategy}", flush=True)

    conn.commit()
    conn.close()
    
def get_controls():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT scope, enabled, flatten_on_disable, updated_at
        FROM trade_controls
        ORDER BY scope
    """)
    rows = cur.fetchall()
    conn.close()

    return {
        r[0]: {
            "enabled": r[1],
            "flatten_on_disable": r[2],
            "updated_at": r[3].isoformat() if r[3] else None,
        }
        for r in rows
    }


def set_control(scope, enabled=None, flatten_on_disable=None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT enabled, flatten_on_disable FROM trade_controls WHERE scope=%s",
        (scope,),
    )
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
            INSERT INTO trade_controls (scope, enabled, flatten_on_disable, updated_at)
            VALUES (%s, %s, %s, NOW())
            """,
            (
                scope,
                True if enabled is None else bool(enabled),
                False if flatten_on_disable is None else bool(flatten_on_disable),
            ),
        )
    else:
        new_enabled = row[0] if enabled is None else bool(enabled)
        new_flatten = row[1] if flatten_on_disable is None else bool(flatten_on_disable)

        cur.execute(
            """
            UPDATE trade_controls
            SET enabled=%s, flatten_on_disable=%s, updated_at=NOW()
            WHERE scope=%s
            """,
            (new_enabled, new_flatten, scope),
        )

    conn.commit()
    conn.close()

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
    conn.close()
    return {
        "assets": assets,
        "controls": get_controls(),
        "last_update": datetime.utcnow().isoformat()
    }