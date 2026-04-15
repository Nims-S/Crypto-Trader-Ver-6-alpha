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


def _insert_trade(cur, symbol, entry, exit_price, pnl, regime="unknown", reason="", confidence=0):
    """
    DB schema in db.py:
      trades(symbol, entry, exit, pnl, regime, reason, confidence, timestamp)
    """
    cur.execute(
        """
        INSERT INTO trades (symbol, entry, exit, pnl, regime, reason, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (symbol, entry, exit_price, round(pnl, 2), regime, reason, confidence),
    )

# ─────────────────────────────────────────────
#  Open position
# ─────────────────────────────────────────────

#DB schema in db.py:
      positions(symbol, entry, sl, tp, size, regime, confidence, updated_at, direction, tp1_hit, tp2_hit)
    """
    direction = (direction or "LONG").upper()
    regime = regime or "unknown"

    if direction == "LONG":
        sl = price - (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 + TP_TIERS[0][0])
        tp2 = price * (1 + TP_TIERS[1][0])
    else:
        sl = price + (atr * 2.0 if atr else price * 0.005)
        tp1 = price * (1 - TP_TIERS[0][0])
        tp2 = price * (1 - TP_TIERS[1][0])


    cur.execute(
        """
        INSERT INTO positions (symbol, entry, sl, tp, size, direction, tp1_hit, tp2_hit)
        VALUES (%s, %s, %s, %s, %s, %s, FALSE, FALSE)
        ON CONFLICT (symbol) DO UPDATE SET
            entry = EXCLUDED.entry,
            sl = EXCLUDED.sl,
            tp = EXCLUDED.tp,
            size = EXCLUDED.size,
            regime = EXCLUDED.regime,
            confidence = EXCLUDED.confidence,
            direction = EXCLUDED.direction,
            tp1_hit = FALSE,
            tp2_hit = FALSE,
            updated_at = CURRENT_TIMESTAMP

        """,
        (symbol, price, round(sl, 4), round(tp1, 4), size, regime, 0, direction),
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
    Expected positions row order from SELECT * positions:
      0 symbol
      1 entry
      2 sl
      3 tp
      4 size
      5 regime
      6 confidence
      7 updated_at
      8 direction
      9 tp1_hit
      10 tp2_hit
    """
    try:
        entry = float(pos[1])
        sl = float(pos[2])
        tp = float(pos[3])
        size = float(pos[4])
        regime = pos[5] if len(pos) > 5 and pos[5] else "unknown"
        confidence = float(pos[6] or 0) if len(pos) > 6 else 0.0
        direction = pos[8] if len(pos) > 8 and pos[8] else "LONG"
        tp1_hit = bool(pos[9]) if len(pos) > 9 else False
        tp2_hit = bool(pos[10]) if len(pos) > 10 else False
        
        is_long = str(direction).upper() == "LONG"

        # Profit per 1 unit of size
        unit_profit = (price - entry) if is_long else (entry - price)
        print(
            f"[MANAGE] {symbol} price={price:.6f} entry={entry:.6f} sl={sl:.6f} "
            f"tp={tp:.6f} size={size:.6f} dir={direction} tp1={tp1_hit} tp2={tp2_hit}",
            flush=True,
        )

        # ── TP1: close 33%, move SL to break-even ───────────────────────────
        if not tp1_hit:
            tp1_price = entry * (1 + TP_TIERS[0][0]) if is_long else entry * (1 - TP_TIERS[0][0])
            hit_tp1 = (price >= tp1_price) if is_long else (price <= tp1_price)

            if hit_tp1:
                close_size = round(size * TP_TIERS[0][1], 6)
                if close_size > 0:
                    pnl1 = unit_profit * close_size
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
                        pnl1,
                        regime=regime,
                        reason="TP1",
                        confidence=confidence,
                    )

                    send_telegram(
                        f"⚡ {symbol} TP1 hit @ {price:.4f} | Closed {close_size:.4f} | "
                        f"PnL: ${pnl1:.2f} | SL → BE"
                    )
                    print(f"[{symbol}] TP1 hit, SL moved to BE, remaining: {remaining}", flush=True)
                return

        # ── TP2: close another 33% of original position ─────────────────────
        if tp1_hit and not tp2_hit:
            tp2_price = entry * (1 + TP_TIERS[1][0]) if is_long else entry * (1 - TP_TIERS[1][0])
            hit_tp2 = (price >= tp2_price) if is_long else (price <= tp2_price)

            if hit_tp2:
                # Close the next 33% of original position, relative to current size after TP1.
                close_size = round(size * (TP_TIERS[1][1] / (1 - TP_TIERS[0][1])), 6)
                close_size = min(close_size, size)

                if close_size > 0:
                    pnl2 = unit_profit * close_size
                    remaining = round(size - close_size, 6)

                    if remaining <= 0:
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
                        pnl2,
                        regime=regime,
                        reason="TP2",
                        confidence=confidence,
                    )

                    send_telegram(
                        f"🎯 {symbol} TP2 hit @ {price:.4f} | Closed {close_size:.4f} | PnL: ${pnl2:.2f}"
                    )
                    print(f"[{symbol}] TP2 hit, remaining: {remaining}", flush=True)
                return

        # ── Trailing stop for runner ─────────────────────────────────────────
        trail_pct = TRAIL_PCT_BULL if is_long else TRAIL_PCT_BEAR
        if unit_profit > 0 and size > 0:
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
                print(f"[{symbol}] Trail SL → {trail_sl:.4f}", flush=True)

        # ── Stop-loss exit ───────────────────────────────────────────────────
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
                regime=regime,
                reason="SL",
                confidence=confidence,
            )
            emoji = "✅" if pnl > 0 else "❌"
            send_telegram(f"{emoji} {symbol} {direction} EXIT @ {price:.4f} | PnL: ${pnl:.2f}")
            print(f"[{symbol}] SL EXIT @ {price:.6f} | PnL: {pnl:.2f}", flush=True)

    except Exception as e:
        print(f"[MANAGE ERROR] {symbol}: {e}", flush=True)
