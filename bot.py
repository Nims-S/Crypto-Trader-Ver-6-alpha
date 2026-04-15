import time
from datetime import datetime

import ccxt
import pandas as pd

from caffeine import push_to_caffeine
from config import CAPITAL, CANDLE_LIMIT, DEFAULT_TIMEFRAME, MAX_COOLDOWN_SECONDS, MAX_POSITIONS, SYMBOLS
from db import get_conn
from execution import manage_position, open_position
from price_feed import feeds
from risk import calculate_position, get_dynamic_capital, risk_gate
from state import get_state, update_asset
from strategy import compute_indicators, generate_signal

exchange = ccxt.binance({
    "enableRateLimit": True,
    "timeout": 15000,
})

try:
    exchange.load_markets()
except Exception as e:
    print(f"[EXCHANGE WARN] load_markets failed: {e}", flush=True)


def fetch_historical_data(symbol: str) -> pd.DataFrame:
    """Fetch historical candles and return an indicator-enriched DataFrame."""
    try:
        bars = exchange.fetch_ohlcv(symbol, timeframe=DEFAULT_TIMEFRAME, limit=CANDLE_LIMIT)
        if not bars:
            return pd.DataFrame()

        df = pd.DataFrame(
            bars,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return compute_indicators(df)
    except Exception as e:
        print(f"[FETCH ERROR] {symbol}: {e}", flush=True)
        return pd.DataFrame()


def load_position(cur, symbol):
    cur.execute(
        """
        SELECT
            symbol, entry, sl, tp, tp2, size, regime, confidence, direction,
            tp1_hit, tp2_hit, strategy, stop_loss_pct, take_profit_pct,
            secondary_take_profit_pct, trail_pct, tp1_close_fraction, tp2_close_fraction
        FROM positions
        WHERE symbol=%s
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None

    return {
        "symbol": row[0],
        "entry": row[1],
        "sl": row[2],
        "tp": row[3],
        "tp2": row[4],
        "size": row[5],
        "regime": row[6],
        "confidence": row[7],
        "direction": row[8],
        "tp1_hit": row[9],
        "tp2_hit": row[10],
        "strategy": row[11],
        "stop_loss_pct": row[12],
        "take_profit_pct": row[13],
        "secondary_take_profit_pct": row[14],
        "trail_pct": row[15],
        "tp1_close_fraction": row[16],
        "tp2_close_fraction": row[17],
    }


def build_position_state(position):
    if not position:
        return None

    return {
        "entry_price": position["entry"],
        "stop_loss": position["sl"],
        "take_profit": position["tp"],
        "take_profit_2": position["tp2"],
        "size": position["size"],
        "strategy": position["strategy"],
    }


def run_bot():
    print("[BOT] LOOP STARTED (v6 alpha)", flush=True)
    last_trade_time = {}

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[HEARTBEAT] Bot alive at {timestamp}", flush=True)

        conn = None
        cur = None

        try:
            conn = get_conn()
            cur = conn.cursor()

            total_cap = get_dynamic_capital(cur, CAPITAL)
            allowed, reason = risk_gate(cur, total_cap)
            if not allowed:
                print(f"[RISK BLOCK] {reason}", flush=True)
                conn.commit()
                time.sleep(12)
                continue

            cur.execute("SELECT COUNT(*) FROM positions")
            active_trades = int(cur.fetchone()[0] or 0)

            for symbol in SYMBOLS:
                feed = feeds.get(symbol)
                if feed is None:
                    continue

                try:
                    price = feed.get_price()
                except Exception as e:
                    print(f"[FEED ERROR] {symbol}: {e}", flush=True)
                    continue

                if price is None or price <= 0:
                    continue

                position = load_position(cur, symbol)
                if position:
                    manage_position(cur, position, price)
                    position = load_position(cur, symbol)

                df = fetch_historical_data(symbol)
                if df.empty:
                    update_asset(
                        symbol=symbol,
                        regime="unknown",
                        strategy="data_unavailable",
                        signal=None,
                        position=build_position_state(position),
                    )
                    continue

                signal = generate_signal(symbol, df)
                strategy_name = signal.strategy
                regime = signal.regime
                update_asset(
                    symbol=symbol,
                    regime=regime,
                    strategy=strategy_name,
                    signal={
                        "side": signal.side if signal else None,
                        "confidence": getattr(signal, "confidence", None),
                    } if signal else None,
                    position=build_position_state(position),
                )

                if signal and signal.side == "LONG" and not position:
                    if active_trades >= MAX_POSITIONS:
                        print(f"[SKIP] Max positions reached. Skipping {symbol}.", flush=True)
                        continue

                    now = time.time()
                    if symbol in last_trade_time and (now - last_trade_time[symbol] < MAX_COOLDOWN_SECONDS):
                        continue

                    size, deployed = calculate_position(
                        symbol=symbol,
                        price=price,
                        total_cap=total_cap,
                        stop_loss_pct=signal.stop_loss_pct,
                        confidence=signal.confidence,
                        regime_multiplier=signal.size_multiplier,
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
                            trail_pct=signal.trail_pct,
                            tp1_close_fraction=signal.tp1_close_fraction,
                            tp2_close_fraction=signal.tp2_close_fraction,
                            confidence=signal.confidence,
                        )

                        update_asset(
                            symbol=symbol,
                            regime=signal.regime,
                            strategy=signal.strategy,
                            signal={
                                "side": signal.side,
                                "confidence": signal.confidence,
                            },
                            position={
                                "entry_price": price,
                                "stop_loss": round(price * (1 - signal.stop_loss_pct), 4),
                                "take_profit": round(price * (1 + signal.take_profit_pct), 4),
                                "take_profit_2": round(price * (1 + signal.secondary_take_profit_pct), 4),
                                "size": size,
                                "strategy": signal.strategy,
                            },
                        )
                        last_trade_time[symbol] = now
                        active_trades += 1

            conn.commit()
            try:
                push_to_caffeine(get_state())
            except Exception as e:
                print(f"[CAFFEINE LOOP ERROR] {e}", flush=True)
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(f"[CRITICAL ERROR] {e}", flush=True)
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        time.sleep(12)


if __name__ == "__main__":
    run_bot()
