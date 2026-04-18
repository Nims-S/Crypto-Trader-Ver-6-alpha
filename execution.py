from math import isfinite

from utils import send_telegram
from performance import log_trade_performance
from state import get_controls


def _insert_trade(cur, symbol, entry, exit_price, pnl, regime="unknown", reason="", confidence=0, strategy="unknown"):
    """Inserts a completed trade into the history table and logs strategy performance."""
    cur.execute(
        """
        INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (symbol, entry, exit_price, round(pnl, 2), regime, reason, confidence),
    )

    try:
        log_trade_performance(strategy, regime, pnl)
    except Exception as e:
        print(f"[PERF LOG ERROR] {symbol}: {e}", flush=True)


def _price_from_pct(entry, pct, is_long):
    if is_long:
        return entry * (1 + pct)
    return entry * (1 - pct)


def _sl_from_pct(entry, pct, is_long):
    if is_long:
        return entry * (1 - pct)
    return entry * (1 + pct)


def _safe_float(value, default=0.0):
    try:
        value = float(value)
        return value if isfinite(value) else default
    except Exception:
        return default


def _normalize_levels(entry, sl, tp1, tp2, tp3, is_long):
    """Force a valid monotonic ladder around entry.

    Long: entry < sl? no, sl must be below entry and tp1 < tp2 < tp3 above entry.
    Short: sl must be above entry and tp1 > tp2 > tp3 below entry.
    """
    entry = _safe_float(entry)
    sl = _safe_float(sl)
    tp1 = _safe_float(tp1)
    tp2 = _safe_float(tp2)
    tp3 = _safe_float(tp3)

    if entry <= 0:
        return entry, sl, tp1, tp2, tp3

    min_risk = entry * 0.0025

    if is_long:
        if not (sl < entry):
            sl = entry - min_risk
        tp1 = max(tp1, entry + min_risk)
        tp2 = max(tp2, tp1 + min_risk)
        if tp3 > 0:
            tp3 = max(tp3, tp2 + min_risk)
    else:
        if not (sl > entry):
            sl = entry + min_risk
        tp1 = min(tp1 if tp1 > 0 else entry - min_risk, entry - min_risk)
        tp2 = min(tp2 if tp2 > 0 else tp1 - min_risk, tp1 - min_risk)
        if tp3 > 0:
            tp3 = min(tp3, tp2 - min_risk)

    return round(entry, 4), round(sl, 4), round(tp1, 4), round(tp2, 4), round(tp3, 4)


def open_position(
    cur,
    symbol,
    price,
    size,
    deployed_capital,
    direction="LONG",
    regime="bull",
    strategy="unknown",
    stop_loss_pct=0.005,
    take_profit_pct=0.01,
    secondary_take_profit_pct=0.02,
    tp3_pct=0.0,
    trail_pct=0.005,
    

    tp1_close_fraction=0.33,
    tp2_close_fraction=0.5,
    tp3_close_fraction=0.0,
    confidence=0,
):
    """Calculates regime-specific SL/TP levels and saves a new position to the DB."""
    # --- KILL SWITCH GUARD ---
    controls = get_controls()

    if not controls.get("GLOBAL", {}).get("enabled", True):
        return

    if not controls.get(symbol, {}).get("enabled", True):
        return
    direction = (direction or "LONG").upper()
    regime = regime or "unknown"
    strategy = strategy or "unknown"
    is_long = direction == "LONG"
    original_size = float(size)

    stop_loss_pct = max(0.0, _safe_float(stop_loss_pct))
    take_profit_pct = max(0.0, _safe_float(take_profit_pct))
    secondary_take_profit_pct = max(0.0, _safe_float(secondary_take_profit_pct))
    tp3_pct = max(0.0, _safe_float(tp3_pct))
    trail_pct = max(0.0, _safe_float(trail_pct))

    sl = _sl_from_pct(price, stop_loss_pct, is_long)
    tp1 = _price_from_pct(price, take_profit_pct, is_long)

    # --- CLAMP LOGIC (ANTI-EXPLOSION GUARD) ---
    tp1_pct = float(take_profit_pct or 0)
    tp2_pct = float(secondary_take_profit_pct or tp1_pct * 1.5)
    tp3_pct = float(tp3_pct or 0)

    # HARD CAPS (CRITICAL)
    tp2_pct = min(tp2_pct, 0.08)   # max 8%
    tp3_pct = min(tp3_pct, 0.15)   # max 15%

    # Build prices
    tp1 = _price_from_pct(price, tp1_pct, is_long)
    tp2 = _price_from_pct(price, tp2_pct, is_long)
    tp3 = _price_from_pct(price, tp3_pct, is_long) if tp3_pct > 0 else 0.0

    price, sl, tp1, tp2, tp3 = _normalize_levels(price, sl, tp1, tp2, tp3, is_long)

    cur.execute(
        """
        INSERT INTO positions (
            symbol, entry, sl, tp, tp2, tp3, size, original_size, direction, regime, confidence,
            strategy, stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
            trail_pct, tp1_close_fraction, tp2_close_fraction, tp3_close_fraction,
            tp1_hit, tp2_hit, tp3_hit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE, FALSE)
        ON CONFLICT (symbol) DO UPDATE SET
            entry = EXCLUDED.entry,
            sl = EXCLUDED.sl,
            tp = EXCLUDED.tp,
            tp2 = EXCLUDED.tp2,
            tp3 = EXCLUDED.tp3,
            size = EXCLUDED.size,
            original_size = EXCLUDED.original_size,
            direction = EXCLUDED.direction,
            regime = EXCLUDED.regime,
            confidence = EXCLUDED.confidence,
            strategy = EXCLUDED.strategy,
            stop_loss_pct = EXCLUDED.stop_loss_pct,
            take_profit_pct = EXCLUDED.take_profit_pct,
            secondary_take_profit_pct = EXCLUDED.secondary_take_profit_pct,
            trail_pct = EXCLUDED.trail_pct,
            tp1_close_fraction = EXCLUDED.tp1_close_fraction,
            tp2_close_fraction = EXCLUDED.tp2_close_fraction,
            tp3_close_fraction = EXCLUDED.tp3_close_fraction,
            tp1_hit = FALSE,
            tp2_hit = FALSE,
            tp3_hit = FALSE,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
             symbol,
            price,
            round(sl, 4),
            round(tp1, 4),
            round(tp2, 4),
            round(tp3, 4),
            size,
            original_size,
            direction,
            regime,
            float(confidence or 0),
            strategy,
            float(stop_loss_pct or 0),
            float(take_profit_pct or 0),
            float(secondary_take_profit_pct or 0),
            float(trail_pct or 0),
            float(tp1_close_fraction or 0.33),
            float(tp2_close_fraction or 0.5),
            float(tp3_close_fraction or 0.0),
        ),
    )

    arrow = "↑" if is_long else "↓"
    msg = (
        f"{arrow} <b>{symbol} {direction}</b> [{regime.upper()} / {strategy}]\n\n"
        f"💰 <b>Deployed:</b> ${deployed_capital:.2f}\n"
        f"📍 <b>Entry:</b> {price:.4f}\n"
        f"🛑 <b>SL:</b> {sl:.4f}\n"
        f"🎯 <b>TP1:</b> {tp1:.4f}\n"
        f"🚀 <b>TP2:</b> {tp2:.4f}"
    )
    send_telegram(msg)


def manage_position(cur, position, price):
    """Manages exits using the stored strategy-specific settings on the position."""
    symbol = position["symbol"]
    
    try:
        entry = float(position["entry"])
        sl = float(position["sl"])
        tp1 = float(position["tp"])
        tp2 = float(position["tp2"] or 0)
        tp3 = float(position.get("tp3") or 0)

        size = float(position["size"])
        original_size = float(position.get("original_size") or size)

        regime = position.get("regime") or "unknown"
        strategy = position.get("strategy") or "unknown"
        confidence = float(position.get("confidence") or 0)
        direction = (position.get("direction") or "LONG").upper()

        tp1_hit = bool(position.get("tp1_hit"))
        tp2_hit = bool(position.get("tp2_hit"))
        tp3_hit = bool(position.get("tp3_hit"))

        trail_pct = float(position.get("trail_pct") or 0)

        tp1_close_fraction = max(0.0, min(1.0, float(position.get("tp1_close_fraction") or 0.33)))
        tp2_close_fraction = max(0.0, min(1.0, float(position.get("tp2_close_fraction") or 0.5)))
        tp3_close_fraction = max(0.0, min(1.0, float(position.get("tp3_close_fraction") or 0.0)))

        is_long = direction == "LONG"
        entry, sl, tp1, tp2, tp3 = _normalize_levels(entry, sl, tp1, tp2, tp3, is_long)
        unit_profit = (price - entry) if is_long else (entry - price)

        if tp2 <= 0:
            fallback_pct = float(position.get("take_profit_pct") or 0) * 1.5
            tp2 = _price_from_pct(entry, fallback_pct, is_long)
            tp2 = _safe_float(tp2)
            if is_long and tp2 <= tp1:
                tp2 = tp1 + max(entry * 0.0025, 1e-8)
            elif (not is_long) and tp2 >= tp1:
                tp2 = tp1 - max(entry * 0.0025, 1e-8)

            cur.execute(
                "UPDATE positions SET tp2=%s WHERE symbol=%s",
                (round(tp2, 4), symbol),
            )
            print(f"[AUTO FIX] {symbol} TP2 repaired → {tp2}", flush=True)

        print(
            f"[DEBUG] {symbol} | price={price:.6f} | entry={entry:.6f} | sl={sl:.6f} | "
            f"tp1={tp1:.6f} | tp2={tp2:.6f} | tp3={tp3:.6f} | size={size:.6f} | "
            f"tp1_hit={tp1_hit} | tp2_hit={tp2_hit} | tp3_hit={tp3_hit}",
            flush=True,
        )

        # -------------------------------------------------
        # TP1
        # -------------------------------------------------
        if not tp1_hit:
            hit_tp1 = (price >= tp1) if is_long else (price <= tp1)
            if hit_tp1:
                close_size = min(size, round(original_size * tp1_close_fraction, 6))
                remaining = round(size - close_size, 6)

                cur.execute(
                    """
                    UPDATE positions
                    SET sl=%s, size=%s, tp1_hit=TRUE, updated_at=CURRENT_TIMESTAMP
                    WHERE symbol=%s
                    """,
                    (entry, remaining, symbol),
                )

                _insert_trade(
                    cur,
                    symbol,
                    entry,
                    price,
                    unit_profit * close_size,
                    regime,
                    f"{strategy}:TP1",
                    confidence,
                    strategy,
                )

                send_telegram(f"⚡ {symbol} TP1 @ {price:.4f} | SL moved to Entry (BE)")

                tp1_hit = True
                size = remaining
                sl = entry

                if size <= 1e-8:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                    return

        # -------------------------------------------------
        # TP2
        # -------------------------------------------------
        if tp1_hit and not tp2_hit and tp2 > 0 and size > 1e-8:
            hit_tp2 = (price >= tp2) if is_long else (price <= tp2)

            # safety: if price has clearly moved beyond TP2, still exit
            hard_tp2 = (price >= tp2 * 1.001) if is_long else (price <= tp2 * 0.999)

            if hit_tp2 or hard_tp2:
                close_size = min(size, round(original_size * tp2_close_fraction, 6))
                remaining = round(size - close_size, 6)

                if remaining <= 1e-8:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute(
                        """
                        UPDATE positions
                        SET size=%s, tp2_hit=TRUE, updated_at=CURRENT_TIMESTAMP
                        WHERE symbol=%s
                        """,
                        (remaining, symbol),
                    )

                _insert_trade(
                    cur,
                    symbol,
                    entry,
                    price,
                    unit_profit * close_size,
                    regime,
                    f"{strategy}:TP2",
                    confidence,
                    strategy,
                )
                print(f"[TP2 EXEC] {symbol} closing {close_size} / {original_size}", flush=True)
                send_telegram(f"🎯 {symbol} TP2 @ {price:.4f} | Partial Exit")

                tp2_hit = True
                size = remaining

                if size <= 1e-8:
                    return
                # --- AGGRESSIVE TRAIL AFTER TP2 ---
                trail_pct *= 0.7
        # -------------------------------------------------
        # TP3
        # -------------------------------------------------
        if tp3 > 0 and not tp3_hit and size > 1e-8:
            hit_tp3 = (price >= tp3) if is_long else (price <= tp3)

            if hit_tp3:
                close_size = min(size, round(original_size * tp3_close_fraction, 6))
                remaining = round(size - close_size, 6)

                if remaining <= 1e-8:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute(
                        """
                        UPDATE positions
                        SET size=%s, tp3_hit=TRUE, updated_at=CURRENT_TIMESTAMP
                        WHERE symbol=%s
                        """,
                        (remaining, symbol),
                    )

                _insert_trade(
                    cur,
                    symbol,
                    entry,
                    price,
                    unit_profit * close_size,
                    regime,
                    f"{strategy}:TP3",
                    confidence,
                    strategy,
                )

                send_telegram(f"🌙 {symbol} TP3 @ {price:.4f} | Moon Exit")

                tp3_hit = True
                size = remaining

                if size <= 1e-8:
                    return

        # -------------------------------------------------
        # Trailing stop after TP1
        # -------------------------------------------------
        if tp1_hit and trail_pct > 0 and size > 1e-8:
            trail_sl = (price * (1 - trail_pct)) if is_long else (price * (1 + trail_pct))

            if (is_long and trail_sl > sl) or ((not is_long) and trail_sl < sl):
                cur.execute(
                    """
                    UPDATE positions
                    SET sl=%s, updated_at=CURRENT_TIMESTAMP
                    WHERE symbol=%s
                    """,
                    (round(trail_sl, 4), symbol),
                )
                sl = trail_sl

        # -------------------------------------------------
        # Stop loss / exit
        # -------------------------------------------------
        hit_sl = (price <= sl) if is_long else (price >= sl)
        if hit_sl and size > 1e-8:
            pnl = unit_profit * size
            cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))

            _insert_trade(
                cur,
                symbol,
                entry,
                price,
                pnl,
                regime,
                f"{strategy}:SL_HIT",
                confidence,
                strategy,
            )

            emoji = "✅" if pnl > 0 else "❌"
            send_telegram(f"{emoji} {symbol} EXIT @ {price:.4f} | PnL: ${pnl:.2f}")

    except Exception as e:
        print(f"[MANAGE ERROR] {symbol}: {e}", flush=True)