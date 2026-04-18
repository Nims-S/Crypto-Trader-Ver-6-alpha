from math import isfinite

from utils import send_telegram
from performance import log_trade_performance
from state import get_controls
from db import get_conn

# =========================
# 🔥 NEW: POSITION RESYNC
# =========================

def update_position_levels(symbol, sl_pct, tp1_pct, tp2_pct, tp3_pct):
    """Force update of SL/TP levels for existing position."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT entry, direction FROM positions WHERE symbol=%s", (symbol,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return

    entry, direction = row
    is_long = (direction or "LONG").upper() == "LONG"

    def _price_from_pct(entry, pct, is_long):
        return entry * (1 + pct) if is_long else entry * (1 - pct)

    def _sl_from_pct(entry, pct, is_long):
        return entry * (1 - pct) if is_long else entry * (1 + pct)

    def _normalize_levels(entry, sl, tp1, tp2, tp3, is_long):
        min_risk = entry * 0.0025
        if is_long:
            if sl >= entry:
                sl = entry - min_risk
            tp1 = max(tp1, entry + min_risk)
            tp2 = max(tp2, tp1 + min_risk)
            if tp3 > 0:
                tp3 = max(tp3, tp2 + min_risk)
        else:
            if sl <= entry:
                sl = entry + min_risk
            tp1 = min(tp1, entry - min_risk)
            tp2 = min(tp2, tp1 - min_risk)
            if tp3 > 0:
                tp3 = min(tp3, tp2 - min_risk)
        return entry, sl, tp1, tp2, tp3

    sl = _sl_from_pct(entry, sl_pct, is_long)
    tp1 = _price_from_pct(entry, tp1_pct, is_long)
    tp2 = _price_from_pct(entry, tp2_pct, is_long)
    tp3 = _price_from_pct(entry, tp3_pct, is_long) if tp3_pct > 0 else 0.0

    entry, sl, tp1, tp2, tp3 = _normalize_levels(entry, sl, tp1, tp2, tp3, is_long)

    cur.execute("""
        UPDATE positions
        SET sl=%s, tp=%s, tp2=%s, tp3=%s, updated_at=NOW()
        WHERE symbol=%s
    """, (round(sl,4), round(tp1,4), round(tp2,4), round(tp3,4), symbol))

    conn.commit()
    conn.close()

    print(f"[SYNC] {symbol} levels updated → tp2={tp2}", flush=True)

# ===== REST OF ORIGINAL FILE BELOW (UNCHANGED) =====

# NOTE: Existing functions continue below unchanged...
