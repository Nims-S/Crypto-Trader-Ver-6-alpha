import time
from datetime import datetime
from caffeine import push_to_caffeine
from state import get_state
import pandas as pd
import ccxt

from price_feed import feeds
from db import get_conn
from config import SYMBOLS, CAPITAL, MAX_COOLDOWN_SECONDS, CANDLE_LIMIT, DEFAULT_TIMEFRAME
from strategy import generate_signal, compute_indicators
from risk import calculate_position, get_dynamic_capital, risk_gate
from execution import open_position, manage_position
from state import update_asset
exchange = ccxt.binance({
    "enableRateLimit": True,
    "timeout": 15000,
})

try:
    exchange.load_markets()
except Exception as e:
    print(f"[EXCHANGE WARN] load_markets failed: {e}", flush=True)


def fetch_historical_data(symbol: str) -> pd.DataFrame:
    """
    Fetch historical candles for a CCXT symbol like 'BTC/USDT'.
    Returns an indicator-enriched DataFrame or an empty DataFrame on failure.
    """
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


def run_bot():
    print("🤖 BOT LOOP STARTED (v6 alpha)", flush=True)
    last_trade_time = {}

    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"❤️ [HEARTBEAT] Bot alive at {timestamp}", flush=True)

        conn = None
        cur = None

        try:
            conn = get_conn()
            cur = conn.cursor()

            # Prevent multiple bot instances from trading at the same time.
            cur.execute("SELECT pg_try_advisory_lock(12345)")
            locked = cur.fetchone()[0]
            if not locked:
                print("[SKIP] Another instance has the lock.", flush=True)
                conn.close()
                conn = None
                time.sleep(10)
                continue

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

                cur.execute("SELECT * FROM positions WHERE symbol=%s", (symbol,))
                pos = cur.fetchone()

                if pos:
                    manage_position(cur, symbol, pos, price)
                    cur.execute("SELECT * FROM positions WHERE symbol=%s", (symbol,))
                    pos = cur.fetchone()

                df = fetch_historical_data(symbol)
                if df.empty:
                    update_asset(
                        symbol=symbol,
                        regime="unknown",
                        strategy="data_unavailable",
                        signal=None,
                        position={
                            "entry_price": pos[1],
                            "stop_loss": pos[2],
                            "take_profit": pos[3],
                            "size": pos[4],
                        } if pos else None,
                    )
                    continue

                signal = generate_signal(symbol, df)

                # ✅ ADD THIS BLOCK RIGHT HERE
                strategy_name = getattr(signal, "strategy", "unknown") if signal else "none"
                regime = getattr(signal, "regime", "unknown") if signal else "unknown"

                position_data = None
                if pos:
                    # Based on your CREATE TABLE positions in db.py:
                    # 0:symbol, 1:entry, 2:sl, 3:tp, 4:size
                    position_data = {
                    "entry_price": pos[1], 
                    "stop_loss": pos[2],   
                    "take_profit": pos[3], 
                    "size": pos[4]         
                }

                update_asset(
                    symbol=symbol,
                    regime=regime,
                    strategy=strategy_name,
                    signal={
                         "side": signal.side if signal else None,
                         "confidence": getattr(signal, "confidence", None)
                    } if signal else None,
                    position=position_data
                )
                # ✅ END BLOCK
                if signal and signal.side == "LONG" and not pos:
                    if active_trades >= 3:
                        print(f"⚠️ Max positions reached. Skipping {symbol}.", flush=True)
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
                    )

                    if size and size > 0:
                        open_position(
                            cur,
                            symbol,
                            price,
                            size,
                            deployed,
                            direction=signal.side,
                            regime=signal.regime,
                            atr=getattr(signal, "atr", None)
                        )

                        # ✅ ADD THIS BLOCK RIGHT AFTER OPEN_POSITION
                        update_asset(
                            symbol=symbol,
                            regime=regime,
                            strategy=strategy_name,
                            signal={
                                 "side": "LONG",
                                 "confidence": signal.confidence
                            },
                            position={
                                "entry_price": price,
                                "size": size,
                                "stop_loss": signal.stop_loss_pct,
                                "take_profit": getattr(signal, "take_profit", None)
                            }
                        )
                        # ✅ END ADD  
                        # ✅ ADD THESE BACK
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
