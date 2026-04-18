from db import get_conn
from utils import send_telegram
from performance import log_trade_performance

# =========================
# PRICE HELPERS
# =========================

def _price_from_pct(entry, pct, is_long):
    return entry * (1 + pct) if is_long else entry * (1 - pct)

def _sl_from_pct(entry, pct, is_long):
    return entry * (1 - pct) if is_long else entry * (1 + pct)

def _normalize_levels(entry, sl, tp1, tp2, tp3, is_long):
    min_gap = entry * 0.0025  # 0.25%

    if is_long:
        if sl >= entry:
            sl = entry - min_gap

        tp1 = max(tp1, entry + min_gap)
        tp2 = max(tp2, tp1 + min_gap)

        if tp3 > 0:
            tp3 = max(tp3, tp2 + min_gap)

    else:
        if sl <= entry:
            sl = entry + min_gap

        tp1 = min(tp1, entry - min_gap)
        tp2 = min(tp2, tp1 - min_gap)

        if tp3 > 0:
            tp3 = min(tp3, tp2 - min_gap)

    return entry, sl, tp1, tp2, tp3


def _bounded_close_size(remaining_size, requested_size):
    remaining = max(0.0, float(remaining_size or 0.0))
    requested = max(0.0, float(requested_size or 0.0))
    return min(remaining, requested)


# =========================
# OPEN POSITION
# =========================

def open_position(
    cur,
    symbol,
    price,
    size,
    deployed_capital,
    direction,
    regime,
    strategy,
    stop_loss_pct,
    take_profit_pct,
    secondary_take_profit_pct,
    tp3_pct,
    tp3_close_fraction,
    trail_pct,
    tp1_close_fraction,
    tp2_close_fraction,
    confidence,
):
    is_long = direction == "LONG"

    sl = _sl_from_pct(price, stop_loss_pct, is_long)
    tp1 = _price_from_pct(price, take_profit_pct, is_long)
    tp2 = _price_from_pct(price, secondary_take_profit_pct, is_long)
    tp3 = _price_from_pct(price, tp3_pct, is_long) if tp3_pct > 0 else 0.0

    price, sl, tp1, tp2, tp3 = _normalize_levels(price, sl, tp1, tp2, tp3, is_long)

    cur.execute("""
        INSERT INTO positions (
            symbol, entry, sl, tp, tp2, tp3, size, original_size,
            regime, confidence, direction, strategy,
            stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
            trail_pct, tp1_close_fraction, tp2_close_fraction, tp3_close_fraction
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        symbol, price, sl, tp1, tp2, tp3, size, size,
        regime, confidence, direction, strategy,
        stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
        trail_pct, tp1_close_fraction, tp2_close_fraction, tp3_close_fraction
    ))

    send_telegram(f"🚀 OPEN {symbol} | Entry={price} | SL={sl} | TP1={tp1} TP2={tp2}")


# =========================
# MANAGE POSITION
# =========================

def manage_position(cur, position, price):
    symbol = position["symbol"]
    is_long = position["direction"] == "LONG"

    entry = position["entry"]
    sl = position["sl"]
    tp1 = position["tp"]
    tp2 = position["tp2"]
    tp3 = position.get("tp3", 0)

    size = float(position["size"])
    original_size = float(position["original_size"])

    tp1_hit = position["tp1_hit"]
    tp2_hit = position["tp2_hit"]
    tp3_hit = position["tp3_hit"]
    regime = position.get("regime", "unknown")
    confidence = float(position.get("confidence", 0) or 0)
    strategy = position.get("strategy", "unknown")

    def record_close(reason, close_price, closed_size):
        closed_size = max(0.0, float(closed_size or 0.0))
        if closed_size <= 0:
            return

        pnl = (float(close_price) - float(entry)) * closed_size if is_long else (float(entry) - float(close_price)) * closed_size
        cur.execute("""
            INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence, strategy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (symbol, entry, close_price, pnl, regime, reason, confidence, strategy))
        log_trade_performance(strategy, regime, pnl)

    # =========================
    # STOP LOSS
    # =========================
    if (is_long and price <= sl) or (not is_long and price >= sl):
        record_close("stop_loss", price, size)
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        send_telegram(f"❌ SL HIT {symbol} at {price}")
        return

    # =========================
    # TP1
    # =========================
    if not tp1_hit and ((is_long and price >= tp1) or (not is_long and price <= tp1)):
        close_size = _bounded_close_size(size, original_size * position["tp1_close_fraction"])
        size -= close_size
        record_close("tp1", price, close_size)

        cur.execute("""
            UPDATE positions SET size=%s, tp1_hit=TRUE WHERE symbol=%s
        """, (size, symbol))

        send_telegram(f"✅ TP1 HIT {symbol} at {price}")

    # =========================
    # TP2
    # =========================
    if not tp2_hit and ((is_long and price >= tp2) or (not is_long and price <= tp2)):
        close_size = _bounded_close_size(size, original_size * position["tp2_close_fraction"])
        size -= close_size
        record_close("tp2", price, close_size)

        cur.execute("""
            UPDATE positions SET size=%s, tp2_hit=TRUE WHERE symbol=%s
        """, (size, symbol))

        send_telegram(f"✅ TP2 HIT {symbol} at {price}")

    if size <= 0:
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        send_telegram(f"🏁 FULLY CLOSED {symbol} at {price}")
        return

    # =========================
    # TP3
    # =========================
    if tp3 and not tp3_hit and ((is_long and price >= tp3) or (not is_long and price <= tp3)):
        record_close("tp3", price, size)
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        send_telegram(f"🏁 TP3 HIT {symbol} at {price}")
        return


# =========================
# 🔥 POSITION RESYNC
# =========================

def update_position_levels(symbol, sl_pct, tp1_pct, tp2_pct, tp3_pct):
    """Force update of SL/TP levels for existing position."""

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT entry, direction, tp3 FROM positions WHERE symbol=%s", (symbol,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return

    entry, direction, current_tp3 = row
    is_long = (direction or "LONG").upper() == "LONG"

    sl = _sl_from_pct(entry, sl_pct, is_long)
    tp1 = _price_from_pct(entry, tp1_pct, is_long)
    tp2 = _price_from_pct(entry, tp2_pct, is_long)
    if tp3_pct > 0:
        tp3 = _price_from_pct(entry, tp3_pct, is_long)
    else:
        tp3 = float(current_tp3 or 0.0)

    entry, sl, tp1, tp2, tp3 = _normalize_levels(entry, sl, tp1, tp2, tp3, is_long)

    cur.execute("""
        UPDATE positions
        SET sl=%s, tp=%s, tp2=%s, tp3=%s, updated_at=NOW()
        WHERE symbol=%s
    """, (round(sl,4), round(tp1,4), round(tp2,4), round(tp3,4), symbol))

    conn.commit()
    conn.close()

    print(f"[SYNC] {symbol} levels updated → tp2={tp2}", flush=True)
