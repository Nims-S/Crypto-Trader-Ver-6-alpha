from utils import send_telegram
from performance import log_trade_performance


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
    trail_pct=0.005,
    tp1_close_fraction=0.33,
    tp2_close_fraction=0.5,
    confidence=0,
):
    """Calculates regime-specific SL/TP levels and saves a new position to the DB."""
    direction = (direction or "LONG").upper()
    regime = regime or "unknown"
    strategy = strategy or "unknown"
    is_long = direction == "LONG"

    sl = _sl_from_pct(price, float(stop_loss_pct or 0), is_long)
    tp1 = _price_from_pct(price, float(take_profit_pct or 0), is_long)
    tp2 = _price_from_pct(price, float(secondary_take_profit_pct or 0), is_long)

    cur.execute(
        """
        INSERT INTO positions (
            symbol, entry, sl, tp, tp2, size, direction, regime, confidence,
            strategy, stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
            trail_pct, tp1_close_fraction, tp2_close_fraction, tp1_hit, tp2_hit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE)
        ON CONFLICT (symbol) DO UPDATE SET
            entry = EXCLUDED.entry,
            sl = EXCLUDED.sl,
            tp = EXCLUDED.tp,
            tp2 = EXCLUDED.tp2,
            size = EXCLUDED.size,
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
            tp1_hit = FALSE,
            tp2_hit = FALSE,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            symbol,
            price,
            round(sl, 4),
            round(tp1, 4),
            round(tp2, 4),
            size,
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
        size = float(position["size"])
        regime = position.get("regime") or "unknown"
        strategy = position.get("strategy") or "unknown"
        confidence = float(position.get("confidence") or 0)
        direction = position.get("direction") or "LONG"
        tp1_hit = bool(position.get("tp1_hit"))
        tp2_hit = bool(position.get("tp2_hit"))
        trail_pct = float(position.get("trail_pct") or 0)
        tp1_close_fraction = float(position.get("tp1_close_fraction") or 0.33)
        tp2_close_fraction = float(position.get("tp2_close_fraction") or 0.5)

        is_long = direction.upper() == "LONG"
        unit_profit = (price - entry) if is_long else (entry - price)

        if not tp1_hit:
            hit_tp1 = (price >= tp1) if is_long else (price <= tp1)
            if hit_tp1:
                close_size = min(size, round(size * tp1_close_fraction, 6))
                remaining = round(size - close_size, 6)
                cur.execute(
                    "UPDATE positions SET sl=%s, size=%s, tp1_hit=TRUE, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s",
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
                return

        if tp1_hit and not tp2_hit and tp2 > 0:
            hit_tp2 = (price >= tp2) if is_long else (price <= tp2)
            if hit_tp2:
                close_size = min(size, round(size * tp2_close_fraction, 6))
                remaining = round(size - close_size, 6)
                if remaining <= 1e-8:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute(
                        "UPDATE positions SET size=%s, tp2_hit=TRUE, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s",
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
                send_telegram(f"🎯 {symbol} TP2 @ {price:.4f} | Partial Exit")
                return

        if unit_profit > 0 and trail_pct > 0:
            trail_sl = (price * (1 - trail_pct)) if is_long else (price * (1 + trail_pct))
            if (is_long and trail_sl > sl) or (not is_long and trail_sl < sl):
                cur.execute(
                    "UPDATE positions SET sl=%s, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s",
                    (round(trail_sl, 4), symbol),
                )
                sl = trail_sl

        hit_sl = (price <= sl) if is_long else (price >= sl)
        if hit_sl:
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
