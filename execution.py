"""
execution.py — position lifecycle management.

Key fixes vs. previous version:
  - update_position_levels() now accepts the caller's `cur` instead of
    opening its own connection (phantom-write bug fix).
  - log_trade_performance() receives `cur` and does not commit independently.
  - Trailing stop is now live: SL moves up as price rises after TP1.
  - Partial-close sizes are clamped to the current DB size to prevent negative
    position sizes when price cascades through multiple TP levels in one tick.
  - Size is re-read from DB after each partial close rather than relying on a
    local variable that may be stale.
"""

from utils import send_telegram
from performance import log_trade_performance


# ── price helpers ─────────────────────────────────────────────────────────────

def _price_from_pct(entry: float, pct: float, is_long: bool) -> float:
    return entry * (1 + pct) if is_long else entry * (1 - pct)


def _sl_from_pct(entry: float, pct: float, is_long: bool) -> float:
    return entry * (1 - pct) if is_long else entry * (1 + pct)


def _normalize_levels(entry, sl, tp1, tp2, tp3, is_long):
    min_gap = entry * 0.0025  # 0.25 %
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


# ── open position ─────────────────────────────────────────────────────────────

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
    sl  = _sl_from_pct(price, stop_loss_pct, is_long)
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
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        symbol, price, sl, tp1, tp2, tp3, size, size,
        regime, confidence, direction, strategy,
        stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
        trail_pct, tp1_close_fraction, tp2_close_fraction, tp3_close_fraction,
    ))
    send_telegram(
        f"🚀 OPEN {symbol} | Entry={price:.4f} | SL={sl:.4f} | "
        f"TP1={tp1:.4f} TP2={tp2:.4f}"
    )


# ── manage position ───────────────────────────────────────────────────────────

def manage_position(cur, position: dict, price: float) -> None:
    symbol        = position["symbol"]
    is_long       = position["direction"] == "LONG"
    entry         = float(position["entry"])
    sl            = float(position["sl"])
    tp1           = float(position["tp"])
    tp2           = float(position["tp2"])
    tp3           = float(position.get("tp3") or 0)
    original_size = float(position["original_size"])
    tp1_hit       = position["tp1_hit"]
    tp2_hit       = position["tp2_hit"]
    tp3_hit       = position["tp3_hit"]
    regime        = position.get("regime", "unknown")
    confidence    = float(position.get("confidence") or 0)
    strategy      = position.get("strategy", "unknown")
    trail_pct     = float(position.get("trail_pct") or 0)

    def current_size() -> float:
        """Re-read size from DB to avoid stale local state."""
        cur.execute("SELECT size FROM positions WHERE symbol=%s", (symbol,))
        row = cur.fetchone()
        return float(row[0]) if row else 0.0

    def record_close(reason: str, close_price: float, closed_size: float) -> None:
        closed_size = max(0.0, float(closed_size or 0.0))
        if closed_size <= 0:
            return
        pnl = (
            (float(close_price) - entry) * closed_size
            if is_long
            else (entry - float(close_price)) * closed_size
        )
        cur.execute("""
            INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence, strategy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (symbol, entry, close_price, pnl, regime, reason, confidence, strategy))
        log_trade_performance(cur, strategy, regime, pnl)

    # ── STOP LOSS ────────────────────────────────────────────────────────────
    if (is_long and price <= sl) or (not is_long and price >= sl):
        size = current_size()
        record_close("stop_loss", price, size)
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        send_telegram(f"❌ SL HIT {symbol} at {price:.4f}")
        return

    # ── TRAILING STOP (activates after TP1 is hit) ───────────────────────────
    if tp1_hit and trail_pct > 0:
        new_trail_sl = (
            price * (1 - trail_pct) if is_long else price * (1 + trail_pct)
        )
        if is_long and new_trail_sl > sl:
            cur.execute(
                "UPDATE positions SET sl=%s, updated_at=NOW() WHERE symbol=%s",
                (round(new_trail_sl, 6), symbol),
            )
        elif not is_long and new_trail_sl < sl:
            cur.execute(
                "UPDATE positions SET sl=%s, updated_at=NOW() WHERE symbol=%s",
                (round(new_trail_sl, 6), symbol),
            )

    # ── TP1 ──────────────────────────────────────────────────────────────────
    if not tp1_hit and (
        (is_long and price >= tp1) or (not is_long and price <= tp1)
    ):
        size = current_size()
        close_size = min(original_size * position["tp1_close_fraction"], size)
        new_size   = max(0.0, size - close_size)
        record_close("tp1", price, close_size)
        cur.execute(
            "UPDATE positions SET size=%s, tp1_hit=TRUE, updated_at=NOW() WHERE symbol=%s",
            (new_size, symbol),
        )
        send_telegram(f"✅ TP1 HIT {symbol} at {price:.4f}")
        tp1_hit = True  # prevent re-entry into this block this tick

    # ── TP2 ──────────────────────────────────────────────────────────────────
    if not tp2_hit and (
        (is_long and price >= tp2) or (not is_long and price <= tp2)
    ):
        size = current_size()
        close_size = min(original_size * position["tp2_close_fraction"], size)
        new_size   = max(0.0, size - close_size)
        record_close("tp2", price, close_size)
        cur.execute(
            "UPDATE positions SET size=%s, tp2_hit=TRUE, updated_at=NOW() WHERE symbol=%s",
            (new_size, symbol),
        )
        send_telegram(f"✅ TP2 HIT {symbol} at {price:.4f}")
        tp2_hit = True

    # ── TP3 (full close of remainder) ────────────────────────────────────────
    if tp3 and not tp3_hit and (
        (is_long and price >= tp3) or (not is_long and price <= tp3)
    ):
        size = current_size()
        record_close("tp3", price, size)
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        send_telegram(f"🏁 TP3 HIT {symbol} at {price:.4f}")
        return


# ── resync position levels ────────────────────────────────────────────────────

def update_position_levels(
    cur,          # ← caller's cursor; no independent connection opened here
    symbol: str,
    sl_pct: float,
    tp1_pct: float,
    tp2_pct: float,
    tp3_pct: float | None,
) -> None:
    """
    Force-update SL/TP levels for an existing position.

    Uses the caller's cursor so it participates in the caller's transaction —
    changes are rolled back automatically if the outer transaction fails.
    """
    cur.execute(
        "SELECT entry, direction, tp3 FROM positions WHERE symbol=%s", (symbol,)
    )
    row = cur.fetchone()
    if not row:
        return

    entry, direction, current_tp3 = row
    is_long = (direction or "LONG").upper() == "LONG"

    sl  = _sl_from_pct(entry, sl_pct, is_long)
    tp1 = _price_from_pct(entry, tp1_pct, is_long)
    tp2 = _price_from_pct(entry, tp2_pct, is_long)

    if tp3_pct is None:
        tp3 = float(current_tp3 or 0.0)
    else:
        tp3 = _price_from_pct(entry, tp3_pct, is_long) if tp3_pct > 0 else 0.0

    entry, sl, tp1, tp2, tp3 = _normalize_levels(entry, sl, tp1, tp2, tp3, is_long)

    cur.execute("""
        UPDATE positions
        SET sl=%s, tp=%s, tp2=%s, tp3=%s, updated_at=NOW()
        WHERE symbol=%s
    """, (round(sl, 6), round(tp1, 6), round(tp2, 6), round(tp3, 6), symbol))
    print(f"[SYNC] {symbol} levels → sl={sl:.4f} tp1={tp1:.4f} tp2={tp2:.4f}", flush=True)
