"""
bot.py — main trading loop with enhanced risk controls.
"""

import time
from datetime import datetime

import ccxt
import pandas as pd

from caffeine import push_to_caffeine
from config import (
    CAPITAL,
    CANDLE_LIMIT,
    DEFAULT_TIMEFRAME,
    MAX_COOLDOWN_SECONDS,
    MAX_POSITIONS,
    SYMBOLS,
)
from db import get_conn
from execution import manage_position, open_position
from price_feed import feeds
from risk import (
    calculate_position,
    get_dynamic_capital,
    get_strategy_multiplier,
    risk_gate,
    evaluate_strategy_pause,
    get_strategy_pause,
    get_symbol_cooldown,
)
from state import get_state, update_asset, get_controls
from strategy import compute_indicators, generate_signal

exchange = ccxt.binance({
    "enableRateLimit": True,
    "timeout": 15000,
})
try:
    exchange.load_markets()
except Exception as e:
    print(f"[EXCHANGE WARN] load_markets failed: {e}", flush=True)


_candle_cache: dict[str, tuple[float, pd.DataFrame]] = {}
CANDLE_CACHE_TTL = 60


def fetch_historical_data(symbol: str) -> pd.DataFrame:
    cached_ts, cached_df = _candle_cache.get(symbol, (0.0, pd.DataFrame()))
    if cached_df is not None and not cached_df.empty and (time.time() - cached_ts) < CANDLE_CACHE_TTL:
        return cached_df

    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe=DEFAULT_TIMEFRAME, limit=CANDLE_LIMIT)
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = compute_indicators(df)
        _candle_cache[symbol] = (time.time(), df)
        return df
    except Exception as e:
        print(f"[FETCH ERROR] {symbol}: {e}", flush=True)
        return cached_df


def load_position(cur, symbol: str):
    cur.execute("""
        SELECT
            symbol, entry, sl, tp, tp2, tp3, size, original_size,
            regime, confidence, direction,
            tp1_hit, tp2_hit, tp3_hit, strategy,
            stop_loss_pct, take_profit_pct, secondary_take_profit_pct,
            trail_pct, tp1_close_fraction, tp2_close_fraction, tp3_close_fraction
        FROM positions
        WHERE symbol=%s FOR UPDATE SKIP LOCKED
    """, (symbol,))
    row = cur.fetchone()
    if not row:
        return None
    return {
        "symbol":                    row[0],
        "entry":                     row[1],
        "sl":                        row[2],
        "tp":                        row[3],
        "tp2":                       row[4],
        "tp3":                       row[5],
        "size":                      float(row[6]),
        "original_size":             float(row[7] or row[6]),
        "regime":                    row[8],
        "confidence":                row[9],
        "direction":                 row[10],
        "tp1_hit":                   row[11],
        "tp2_hit":                   row[12],
        "tp3_hit":                   row[13],
        "strategy":                  row[14],
        "stop_loss_pct":             row[15],
        "take_profit_pct":           row[16],
        "secondary_take_profit_pct": row[17],
        "trail_pct":                 row[18],
        "tp1_close_fraction":        row[19],
        "tp2_close_fraction":        row[20],
        "tp3_close_fraction":        row[21],
    }


def build_position_state(position):
    if not position:
        return None
    return {
        "entry_price":  position["entry"],
        "stop_loss":    position["sl"],
        "take_profit":  position["tp"],
        "take_profit_2": position["tp2"],
        "take_profit_3": position.get("tp3"),
        "size":         position["size"],
        "original_size": position.get("original_size"),
        "strategy":     position["strategy"],
    }


def run_bot():
    print("[BOT] LOOP STARTED (v6 hardened++)", flush=True)
    last_trade_time: dict[str, float] = {}

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[HEARTBEAT] Bot alive at {timestamp}", flush=True)

        conn = None
        cur  = None
        try:
            conn = get_conn()
            cur  = conn.cursor()

            total_cap = get_dynamic_capital(cur, CAPITAL)

            allowed, reason = risk_gate(cur, total_cap)
            if not allowed:
                print(f"[RISK BLOCK] {reason}", flush=True)
                conn.commit()
                time.sleep(3)
                continue

            for symbol in SYMBOLS:
                feed = feeds.get(symbol)
                if feed is None:
                    continue

                price = None
                try:
                    price = feed.get_price()
                except Exception as e:
                    print(f"[FEED ERROR] {symbol}: {e}", flush=True)

                # ── data health gate ────────────────────────────────────────
                if price is None or price <= 0 or getattr(feed, "is_stale", lambda *_: False)(20):
                    update_asset(
                        symbol=symbol,
                        regime="data_stale",
                        strategy="feed_unavailable",
                        signal=None,
                        position=None,
                    )
                    continue

                position = load_position(cur, symbol)

                controls      = get_controls()
                global_ctrl   = controls.get("GLOBAL", {})
                symbol_ctrl   = controls.get(symbol, {})
                global_enabled = global_ctrl.get("enabled", True)
                symbol_enabled = symbol_ctrl.get("enabled", True)
                blocked = (not global_enabled) or (not symbol_enabled)

                if position:
                    manage_position(cur, position, price)
                    position = load_position(cur, symbol)

                if blocked:
                    update_asset(symbol, "paused", "kill_switch", None, build_position_state(position))
                    continue

                df = fetch_historical_data(symbol)
                if df.empty:
                    update_asset(symbol, "unknown", "data_unavailable", None, build_position_state(position))
                    continue

                signal = generate_signal(symbol, df)

                # ── strategy pause check ────────────────────────────────────
                if signal and signal.strategy != "no_trade":
                    pause = evaluate_strategy_pause(cur, signal.strategy)
                    if pause:
                        update_asset(symbol, "paused", "strategy_paused", None, build_position_state(position))
                        continue

                # ── symbol cooldown check ───────────────────────────────────
                cooldown = get_symbol_cooldown(cur, symbol)
                if cooldown:
                    update_asset(symbol, "paused", "symbol_cooldown", None, build_position_state(position))
                    continue

                update_asset(
                    symbol=symbol,
                    regime=signal.regime if signal else "unknown",
                    strategy=signal.strategy if signal else "none",
                    signal={
                        "side": signal.side if signal else None,
                        "confidence": getattr(signal, "confidence", None),
                    } if signal else None,
                    position=build_position_state(position),
                )

                if (
                    signal
                    and signal.side == "LONG"
                    and signal.strategy != "no_trade"
                    and not position
                ):
                    cur.execute("SELECT COUNT(*) FROM positions")
                    active_trades = int(cur.fetchone()[0] or 0)
                    if active_trades >= MAX_POSITIONS:
                        continue

                    now = time.time()
                    if symbol in last_trade_time and (
                        now - last_trade_time[symbol] < MAX_COOLDOWN_SECONDS
                    ):
                        continue

                    strategy_mult = get_strategy_multiplier(cur, signal.strategy, signal.regime)
                    size, deployed = calculate_position(
                        symbol=symbol,
                        price=price,
                        total_cap=total_cap,
                        stop_loss_pct=signal.stop_loss_pct,
                        confidence=signal.confidence,
                        regime_multiplier=strategy_mult,
                        size_multiplier=float(getattr(signal, "size_multiplier", 1.0) or 1.0),
                    )

                    if size and size > 0:
                        open_position(
                            cur=cur,
                            symbol=symbol,
                            price=price,
                            size=size,
                            deployed_capital=deployed,
                            direction=signal.side,
                            regime=signal.regime,
                            strategy=signal.strategy,
                            stop_loss_pct=signal.stop_loss_pct,
                            take_profit_pct=signal.take_profit_pct,
                            secondary_take_profit_pct=signal.secondary_take_profit_pct,
                            tp3_pct=signal.tp3_pct,
                            tp3_close_fraction=signal.tp3_close_fraction,
                            trail_pct=signal.trail_pct,
                            tp1_close_fraction=signal.tp1_close_fraction,
                            tp2_close_fraction=signal.tp2_close_fraction,
                            confidence=signal.confidence,
                        )
                        print(f"[ENTRY] {symbol}", flush=True)
                        last_trade_time[symbol] = now

            conn.commit()

            try:
                state = get_state()
                if state.get("assets"):
                    push_to_caffeine(state)
            except Exception as e:
                print(f"[CAFFEINE ERROR] {e}", flush=True)

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"[CRITICAL ERROR] {e}", flush=True)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        time.sleep(3)


if __name__ == "__main__":
    run_bot()
