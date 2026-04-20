# patched bot with dynamic ATR trailing
import time
from datetime import datetime
import ccxt
import pandas as pd
from caffeine import push_to_caffeine
from config import CAPITAL,CANDLE_LIMIT,DEFAULT_TIMEFRAME,MAX_COOLDOWN_SECONDS,MAX_POSITIONS,SYMBOLS
from db import get_conn
from execution import manage_position, open_position
from price_feed import feeds
from risk import calculate_position,get_dynamic_capital,get_strategy_multiplier,risk_gate,evaluate_strategy_pause,get_symbol_cooldown
from state import get_state, update_asset, get_controls
from strategy import compute_indicators, generate_signal

exchange = ccxt.binance({"enableRateLimit": True,"timeout": 15000})
try: exchange.load_markets()
except Exception as e: print(f"[EXCHANGE WARN] {e}", flush=True)

_candle_cache={}
CANDLE_CACHE_TTL=60

def fetch_historical_data(symbol):
    cached_ts, cached_df = _candle_cache.get(symbol,(0.0,pd.DataFrame()))
    if not cached_df.empty and (time.time()-cached_ts)<CANDLE_CACHE_TTL:
        return cached_df
    try:
        bars=exchange.fetch_ohlcv(symbol,timeframe=DEFAULT_TIMEFRAME,limit=CANDLE_LIMIT)
        df=pd.DataFrame(bars,columns=["timestamp","open","high","low","close","volume"])
        df["timestamp"]=pd.to_datetime(df["timestamp"],unit="ms",utc=True)
        df=compute_indicators(df)
        _candle_cache[symbol]=(time.time(),df)
        return df
    except Exception:
        return cached_df

def load_position(cur,symbol):
    cur.execute("SELECT symbol,entry,sl,tp,tp2,tp3,size,original_size,regime,confidence,direction,tp1_hit,tp2_hit,tp3_hit,strategy,stop_loss_pct,take_profit_pct,secondary_take_profit_pct,trail_pct,trail_atr_mult,tp1_close_fraction,tp2_close_fraction,tp3_close_fraction FROM positions WHERE symbol=%s FOR UPDATE SKIP LOCKED",(symbol,))
    r=cur.fetchone()
    if not r: return None
    return {"symbol":r[0],"entry":r[1],"sl":r[2],"tp":r[3],"tp2":r[4],"tp3":r[5],"size":float(r[6]),"original_size":float(r[7] or r[6]),"regime":r[8],"confidence":r[9],"direction":r[10],"tp1_hit":r[11],"tp2_hit":r[12],"tp3_hit":r[13],"strategy":r[14],"stop_loss_pct":r[15],"take_profit_pct":r[16],"secondary_take_profit_pct":r[17],"trail_pct":r[18],"trail_atr_mult":r[19],"tp1_close_fraction":r[20],"tp2_close_fraction":r[21],"tp3_close_fraction":r[22]}

def run_bot():
    print("[BOT START]")
    last_trade_time={}
    while True:
        conn=None;cur=None
        try:
            conn=get_conn();cur=conn.cursor()
            total_cap=get_dynamic_capital(cur,CAPITAL)
            ok,_=risk_gate(cur,total_cap)
            if not ok: conn.commit(); time.sleep(3); continue
            for symbol in SYMBOLS:
                feed=feeds.get(symbol)
                if not feed: continue
                price=feed.get_price()
                if not price or price<=0: continue
                pos=load_position(cur,symbol)
                df=fetch_historical_data(symbol)
                atr=float(df.iloc[-1]["atr_pct"]) if not df.empty else None
                if pos:
                    manage_position(cur,pos,price,atr)
                    pos=load_position(cur,symbol)
                if df.empty: continue
                signal=generate_signal(symbol,df)
                if signal and signal.side=="LONG" and not pos:
                    size,dep=calculate_position(symbol,price,total_cap,signal.stop_loss_pct,signal.confidence,1.0,signal.size_multiplier)
                    if size>0:
                        open_position(cur,symbol,price,size,dep,signal.side,signal.regime,signal.strategy,signal.stop_loss_pct,signal.take_profit_pct,signal.secondary_take_profit_pct,signal.tp3_pct,signal.tp3_close_fraction,signal.trail_pct,signal.trail_atr_mult,signal.tp1_close_fraction,signal.tp2_close_fraction,signal.confidence)
            conn.commit()
        except Exception as e:
            if conn: conn.rollback()
            print(e)
        finally:
            if cur: cur.close()
            if conn: conn.close()
        time.sleep(3)

if __name__=="__main__": run_bot()