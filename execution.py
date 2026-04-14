from utils import send_telegram

def open_position(cur, symbol, price, size, deployed_capital, signal):
    sl = price * (1 - signal.stop_loss_pct)
    tp1 = price * (1 + signal.take_profit_pct)

    cur.execute("""
        INSERT INTO positions (symbol, entry, sl, tp, size, regime, confidence, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (symbol) DO UPDATE SET
            entry = EXCLUDED.entry,
            sl = EXCLUDED.sl,
            tp = EXCLUDED.tp,
            size = EXCLUDED.size,
            regime = EXCLUDED.regime,
            confidence = EXCLUDED.confidence,
            updated_at = CURRENT_TIMESTAMP
    """, (symbol, price, sl, tp1, size, signal.regime, signal.confidence))

    message = (
        f"🚀 <b>{symbol} {signal.side}</b>\n\n"
        f"🧠 <b>Regime:</b> {signal.regime}\n"
        f"⭐ <b>Confidence:</b> {signal.confidence:.2f}\n"
        f"💰 <b>Deployed:</b> ${deployed_capital:.2f}\n"
        f"📍 <b>Entry:</b> {price:.2f}\n"
        f"🛑 <b>SL:</b> {sl:.2f}\n"
        f"🎯 <b>TP:</b> {tp1:.2f}\n"
        f"📝 <b>Reason:</b> {signal.reason}"
    )
    send_telegram(message)

def manage_position(cur, symbol, pos, price):
    # pos: symbol, entry, sl, tp, size, regime, confidence, updated_at
    entry, sl, tp, size = pos[1], pos[2], pos[3], pos[4]
    regime = pos[5] if len(pos) > 5 else "unknown"
    confidence = pos[6] if len(pos) > 6 and pos[6] is not None else 0.0

    moved_to_breakeven = False

    # Break-even activation near 1R
    trigger_price = entry + (entry - sl)
    if price >= trigger_price and sl < entry:
        cur.execute("UPDATE positions SET sl=%s, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s", (entry, symbol))
        send_telegram(f"⚡ {symbol} BE activated ({regime})")
        print(f"[{symbol}] BE activated", flush=True)
        moved_to_breakeven = True

    # Trailing stop once the trade is working
    if price > entry:
        trail_sl = price * 0.995
        new_sl = max(sl, trail_sl)
        if new_sl > sl:
            cur.execute("UPDATE positions SET sl=%s, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s", (new_sl, symbol))
            print(f"[{symbol}] Trailing SL updated to {new_sl:.2f}", flush=True)
            sl = new_sl

    # Profit target or stop loss exit
    exit_reason = None
    if price <= sl:
        exit_reason = "STOP"
    elif price >= tp:
        exit_reason = "TP"

    if exit_reason:
        pnl = (price - entry) * size
        cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
        cur.execute("""
            INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (symbol, entry, price, pnl, regime, exit_reason, confidence))

        status_emoji = "✅" if pnl > 0 else "❌"
        send_telegram(f"{status_emoji} {symbol} EXIT @ {price:.2f}\nPnL: ${pnl:.2f}\nReason: {exit_reason}")
        print(f"[{symbol}] EXIT {exit_reason} at {price} | PnL: {pnl}", flush=True)
