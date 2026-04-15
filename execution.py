from utils import send_telegram

# Configuration
TP_TIERS = [
    (0.0075, 0.33),   # TP1: +0.75% → close 33%, move SL to break-even
    (0.020,  0.33),   # TP2: +2.0%  → close another 33%
]
TRAIL_PCT_BULL = 0.010   # 1.0% trail in bull
TRAIL_PCT_BEAR = 0.005   # 0.5% trail for shorts in bear

def _insert_trade(cur, symbol, entry, exit_price, pnl, regime="unknown", reason="", confidence=0):
    """Inserts a completed trade into the history table."""
    cur.execute(
        """
        INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (symbol, entry, exit_price, round(pnl, 2), regime, reason, confidence),
    )

def open_position(cur, symbol, price, size, deployed_capital, direction="LONG", atr=None, regime="bull", confidence=0):
    """Calculates SL/TP and saves a new position to the DB."""
    direction = (direction or "LONG").upper()
    regime = regime or "unknown"

    # Use ATR for SL if available, otherwise 0.5% default
    sl_offset = (atr * 2.0) if (atr and atr > 0) else (price * 0.005)

    if direction == "LONG":
        sl = price - sl_offset
        tp1 = price * (1 + TP_TIERS[0][0])
    else:
        sl = price + sl_offset
        tp1 = price * (1 - TP_TIERS[0][0])

    # Corrected INSERT: Matches the 10 columns in your DB schema
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
        (symbol, price, round(sl, 4), round(tp1, 4), size, direction, regime, float(confidence or 0)),
    )

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
    """
    Manages TP/SL logic based on the 'pos' tuple from SELECT * FROM positions.
    Indices based on db.py: 0:sym, 1:entry, 2:sl, 3:tp, 4:size, 5:regime, 6:conf, 8:dir, 9:tp1, 10:tp2
    """
    try:
        entry = float(pos[1])
        sl = float(pos[2])
        size = float(pos[4])
        regime = pos[5] if pos[5] else "unknown"
        confidence = float(pos[6] or 0)
        direction = pos[8] if pos[8] else "LONG"
        tp1_hit = bool(pos[9])
        tp2_hit = bool(pos[10])
        
        is_long = (direction.upper() == "LONG")
        unit_profit = (price - entry) if is_long else (entry - price)

        # 1. Handle TP1 (Move SL to Break Even)
        if not tp1_hit:
            tp1_price = entry * (1 + TP_TIERS[0][0]) if is_long else entry * (1 - TP_TIERS[0][0])
            hit_tp1 = (price >= tp1_price) if is_long else (price <= tp1_price)
            
            if hit_tp1:
                close_size = round(size * TP_TIERS[0][1], 6)
                remaining = round(size - close_size, 6)
                # Move SL to entry (Break Even)
                cur.execute(
                    "UPDATE positions SET sl=%s, size=%s, tp1_hit=TRUE, updated_at=CURRENT_TIMESTAMP WHERE symbol=%s",
                    (entry, remaining, symbol),
                )
                _insert_trade(cur, symbol, entry, price, (unit_profit * close_size), regime, "TP1", confidence)
                send_telegram(f"⚡ {symbol} TP1 @ {price:.4f} | SL moved to Entry (BE)")
                return

        # 2. Handle TP2
        if tp1_hit and not tp2_hit:
            tp2_price = entry * (1 + TP_TIERS[1][0]) if is_long else entry * (1 - TP_TIERS[1][0])
            hit_tp2 = (price >= tp2_price) if is_long else (price <= tp2_price)
            
            if hit_tp2:
                # Calculate relative size to close another 33% of original
                close_size = min(size, round(size * 0.5, 6)) # Approx remaining relative half
                remaining = round(size - close_size, 6)
                
                if remaining <= 1e-8:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute("UPDATE positions SET size=%s, tp2_hit=TRUE WHERE symbol=%s", (remaining, symbol))
                
                _insert_trade(cur, symbol, entry, price, (unit_profit * close_size), regime, "TP2", confidence)
                send_telegram(f"🎯 {symbol} TP2 @ {price:.4f} | Partial Exit")
                return

        # 3. Trailing Stop Logic (Only if in profit)
        trail_pct = TRAIL_PCT_BULL if is_long else TRAIL_PCT_BEAR
        if unit_profit > 0:
            trail_sl = (price * (1 - trail_pct)) if is_long else (price * (1 + trail_pct))
            if (is_long and trail_sl > sl) or (not is_long and trail_sl < sl):
                cur.execute("UPDATE positions SET sl=%s WHERE symbol=%s", (round(trail_sl, 4), symbol))

        # 4. Final Stop Loss (Hit SL)
        hit_sl = (price <= sl) if is_long else (price >= sl)
        if hit_sl:
            pnl = unit_profit * size
            cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
            _insert_trade(cur, symbol, entry, price, pnl, regime, "SL_HIT", confidence)
            emoji = "✅" if pnl > 0 else "❌"
            send_telegram(f"{emoji} {symbol} EXIT @ {price:.4f} | PnL: ${pnl:.2f}")

    except Exception as e:
        print(f"[MANAGE ERROR] {symbol}: {e}", flush=True)
