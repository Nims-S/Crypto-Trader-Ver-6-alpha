from utils import send_telegram

TP_TIERS = [
    (0.0075, 0.33),   # TP1: +0.75% → close 33%, move SL to break-even
    (0.020,  0.33),   # TP2: +2.0%  → close another 33%
]
TRAIL_PCT_BULL = 0.010   
TRAIL_PCT_BEAR = 0.005   

def _insert_trade(cur, symbol, entry, exit_price, pnl, regime="unknown", reason="", confidence=0):
    cur.execute(
        """
        INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (symbol, entry, exit_price, round(pnl, 2), regime, reason, confidence),
    )

def open_position(cur, symbol, price, size, deployed_capital, direction="LONG", atr=None, regime="bull"):
    direction = (direction or "LONG").upper()
    regime = regime or "unknown"

    if direction == "LONG":
        sl = price - (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 + TP_TIERS[0][0])
    else:
        sl = price + (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 - TP_TIERS[0][0])

    cur.execute(
        """
        INSERT INTO positions (symbol, entry, sl, tp, size, direction, regime, confidence, tp1_hit, tp2_hit)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE)
        ON CONFLICT (symbol) DO UPDATE SET
            entry = EXCLUDED.entry,
            sl = EXCLUDED.sl,
            tp = EXCLUDED.tp,
            size = EXCLUDED.size,
            direction = EXCLUDED.direction,
            regime = EXCLUDED.regime,
            confidence = EXCLUDED.confidence,
            tp1_hit = FALSE,
            tp2_hit = FALSE,
            updated_at = CURRENT_TIMESTAMP
        """,
        (symbol, price, round(sl, 4), round(tp1, 4), size, direction, regime, 0),
    )
    # ... rest of function (Telegram msg, etc.)


    arrow = "↑" if direction == "LONG" else "↓"
    msg = (
        f"{arrow} <b>{symbol} {direction}</b> [{regime.upper()}]\n\n"
        f"💰 <b>Deployed:</b> ${deployed_capital:.2f}\n"
        f"📍 <b>Entry:</b> {price:.4f}\n"
        f"🛑 <b>SL:</b> {sl:.4f}\n"
        f"🎯 <b>TP1:</b> {tp1:.4f}"
    )
    send_telegram(msg)

def manage_position(cur, symbol, pos, price):
    try:
        entry = float(pos[1])
        sl = float(pos[2])
        size = float(pos[4])
        regime = pos[5] if len(pos) > 5 and pos[5] else "unknown"
        confidence = float(pos[6] or 0) if len(pos) > 6 else 0.0
        direction = pos[8] if len(pos) > 8 and pos[8] else "LONG"
        tp1_hit = bool(pos[9]) if len(pos) > 9 else False
        tp2_hit = bool(pos[10]) if len(pos) > 10 else False
        is_long = str(direction).upper() == "LONG"
        unit_profit = (price - entry) if is_long else (entry - price)

        if not tp1_hit:
            tp1_price = entry * (1 + TP_TIERS[0][0]) if is_long else entry * (1 - TP_TIERS[0][0])
            hit_tp1 = (price >= tp1_price) if is_long else (price <= tp1_price)
            if hit_tp1:
                close_size = round(size * TP_TIERS[0][1], 6)
                remaining = round(size - close_size, 6)
                cur.execute(
                    "UPDATE positions SET sl=%s, size=%s, tp1_hit=TRUE, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s",
                    (entry, remaining, symbol),
                )
                _insert_trade(cur, symbol, entry, price, (unit_profit * close_size), regime, "TP1", confidence)
                send_telegram(f"⚡ {symbol} TP1 @ {price:.4f} | SL → BE")
                return

        if tp1_hit and not tp2_hit:
            tp2_price = entry * (1 + TP_TIERS[1][0]) if is_long else entry * (1 - TP_TIERS[1][0])
            hit_tp2 = (price >= tp2_price) if is_long else (price <= tp2_price)
            if hit_tp2:
                close_size = round(size * (TP_TIERS[1][1] / (1 - TP_TIERS[0][1])), 6)
                remaining = round(size - close_size, 6)
                if remaining <= 0:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute("UPDATE positions SET size=%s, tp2_hit=TRUE WHERE symbol=%s", (remaining, symbol))
                _insert_trade(cur, symbol, entry, price, (unit_profit * close_size), regime, "TP2", confidence)
                send_telegram(f"🎯 {symbol} TP2 @ {price:.4f}")
                return

        trail_pct = TRAIL_PCT_BULL if is_long else TRAIL_PCT_BEAR
        if unit_profit > 0 and size > 0:
            trail_sl = (price * (1 - trail_pct)) if is_long else (price * (1 + trail_pct))
            if (is_long and trail_sl > sl) or ((not is_long) and trail_sl < sl):
                cur.execute("UPDATE positions SET sl=%s WHERE symbol=%s", (round(trail_sl, 4), symbol))

        hit_sl = (price <= sl) if is_long else (price >= sl)
        if hit_sl:
            pnl = unit_profit * size
            cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
            _insert_trade(cur, symbol, entry, price, pnl, regime, "SL", confidence)
            emoji = "✅" if pnl > 0 else "❌"
            send_telegram(f"{emoji} {symbol} EXIT @ {price:.4f} | PnL: ${pnl:.2f}")

    except Exception as e:
        print(f"[MANAGE ERROR] {symbol}: {e}", flush=True)