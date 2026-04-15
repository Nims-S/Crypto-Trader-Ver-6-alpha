from utils import send_telegram

# ─────────────────────────────────────────────
#  Take-profit tiers
# ─────────────────────────────────────────────
# Each tier: (price_pct_gain, fraction_of_position_to_close)
TP_TIERS = [
    (0.0075, 0.33),   # TP1: +0.75% → close 33%, move SL to break-even
    (0.020,  0.33),   # TP2: +2.0%  → close another 33%
    # TP3: trail the remaining 34% with a 1% trailing stop (handled in manage_position)
]

# Trailing stop distance for the residual position after TP2
TRAIL_PCT_BULL = 0.010   # 1.0% trail in bull
TRAIL_PCT_BEAR = 0.005   # 0.5% trail for shorts in bear


def _insert_trade(cur, symbol, entry, exit_price, pnl, direction):
    """Helper to record closed/partial-close trades consistently."""
    cur.execute(
        "INSERT INTO trades (symbol, entry, exit, pnl, direction) VALUES (%s, %s, %s, %s, %s)",
        (symbol, entry, exit_price, round(pnl, 2), direction),
    )


# ─────────────────────────────────────────────
#  Open position
# ─────────────────────────────────────────────

def open_position(cur, symbol, price, size, deployed_capital, direction="LONG", atr=None, regime="bull"):
    """Insert a new position. SL is ATR-based; initial TP1 stored in 'tp' column."""
    direction = (direction or "LONG").upper()

    if direction == "LONG":
        sl = price - (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 + TP_TIERS[0][0])
    else:  # SHORT
        sl = price + (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 - TP_TIERS[0][0])

    cur.execute(
        """
        INSERT INTO positions (symbol, entry, sl, tp, size, direction, tp1_hit, tp2_hit)
        VALUES (%s, %s, %s, %s, %s, %s, FALSE, FALSE)
        """,
        (symbol, price, round(sl, 4), round(tp1, 4), size, direction),
    )

    arrow = "↑" if direction == "LONG" else "↓"
    msg = (
        f"{arrow} <b>{symbol} {direction}</b> [{regime.upper()}]\n\n"
        f"💰 <b>Deployed:</b> ${deployed_capital:.2f}\n"
        f"📍 <b>Entry:</b> {price:.4f}\n"
        f"🛑 <b>SL:</b> {sl:.4f}\n"
        f"🎯 <b>TP1:</b> {tp1:.4f}\n"
        f"🎯 <b>TP2:</b> {price * (1 + TP_TIERS[1][0]) if direction == 'LONG' else price * (1 - TP_TIERS[1][0]):.4f}"
    )
    send_telegram(msg)


# ─────────────────────────────────────────────
#  Manage open position
# ─────────────────────────────────────────────

def manage_position(cur, symbol, pos, price):
    """
    pos columns:
      0:symbol 1:entry 2:sl 3:tp 4:size 5:direction 6:tp1_hit 7:tp2_hit
    """
    try:
        entry = float(pos[1])
        sl = float(pos[2])
        tp = float(pos[3])
        size = float(pos[4])
        direction = pos[5] if len(pos) > 5 and pos[5] else "LONG"
        tp1_hit = bool(pos[6]) if len(pos) > 6 else False
        tp2_hit = bool(pos[7]) if len(pos) > 7 else False

        is_long = direction == "LONG"
        profit = (price - entry) if is_long else (entry - price)

        # ── TP1: +0.75% → close 33%, move SL to break-even ──────────────────
        if not tp1_hit:
            tp1_price = entry * (1 + TP_TIERS[0][0]) if is_long else entry * (1 - TP_TIERS[0][0])
            if (is_long and price >= tp1_price) or ((not is_long) and price <= tp1_price):
                close_size = round(size * TP_TIERS[0][1], 6)
                pnl1 = profit * close_size
                remaining = round(size - close_size, 6)

                cur.execute(
                    "UPDATE positions SET sl=%s, size=%s, tp1_hit=TRUE WHERE symbol=%s",
                    (entry, remaining, symbol),
                )
                _insert_trade(cur, symbol, entry, price, pnl1, direction)

                send_telegram(
                    f"⚡ {symbol} TP1 hit @ {price:.4f} | Closed {close_size:.4f} | PnL: ${pnl1:.2f} | SL → BE"
                )
                print(f"[{symbol}] TP1 hit, SL moved to BE, remaining: {remaining}", flush=True)
                return

        # ── TP2: +2.0% → close another 33% ──────────────────────────────────
        if tp1_hit and not tp2_hit:
            tp2_price = entry * (1 + TP_TIERS[1][0]) if is_long else entry * (1 - TP_TIERS[1][0])
            if (is_long and price >= tp2_price) or ((not is_long) and price <= tp2_price):
                # Close another 33% of the ORIGINAL position, which is 33 / remaining_after_tp1 of the current size
                close_size = round(size * (TP_TIERS[1][1] / (1 - TP_TIERS[0][1])), 6)
                close_size = min(close_size, size)
                pnl2 = profit * close_size
                remaining = round(size - close_size, 6)

                if remaining <= 0:
                    cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
                else:
                    cur.execute(
                        "UPDATE positions SET size=%s, tp2_hit=TRUE WHERE symbol=%s",
                        (remaining, symbol),
                    )
                _insert_trade(cur, symbol, entry, price, pnl2, direction)

                send_telegram(f"🎯 {symbol} TP2 hit @ {price:.4f} | Closed {close_size:.4f} | PnL: ${pnl2:.2f}")
                print(f"[{symbol}] TP2 hit, remaining: {remaining}", flush=True)
                return

        # ── Trailing stop (TP3 / residual position) ───────────────────────────
        trail_pct = TRAIL_PCT_BULL if is_long else TRAIL_PCT_BEAR
        if profit > 0 and size > 0:
            trail_sl = (price * (1 - trail_pct)) if is_long else (price * (1 + trail_pct))
            if (is_long and trail_sl > sl) or ((not is_long) and trail_sl < sl):
                cur.execute("UPDATE positions SET sl=%s WHERE symbol=%s", (round(trail_sl, 4), symbol))
                print(f"[{symbol}] Trail SL → {trail_sl:.4f}", flush=True)

        # ── Stop-loss exit ────────────────────────────────────────────────────
        hit_sl = (is_long and price <= sl) or ((not is_long) and price >= sl)
        if hit_sl:
            pnl = profit * size
            cur.execute("DELETE FROM positions WHERE symbol=%s", (symbol,))
            _insert_trade(cur, symbol, entry, price, pnl, direction)
            emoji = "✅" if pnl > 0 else "❌"
            send_telegram(f"{emoji} {symbol} {direction} EXIT @ {price:.4f} | PnL: ${pnl:.2f}")
            print(f"[{symbol}] SL EXIT @ {price} | PnL: {pnl:.2f}", flush=True)

    except Exception as e:
        print(f"[MANAGE ERROR] {symbol}: {e}", flush=True)