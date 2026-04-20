from __future__ import annotations

import argparse
import json
import math
import random
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Dict, List, Tuple

import ccxt
import numpy as np
import pandas as pd

try:
    from strategy import compute_indicators
except Exception:
    compute_indicators = None

try:
    from risk import calculate_position
except Exception:
    calculate_position = None


DATA_DIR = Path("backtest_data")
RESULT_DIR = Path("backtest_results")
DATA_DIR.mkdir(exist_ok=True)
RESULT_DIR.mkdir(exist_ok=True)

DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
MIN_NOTIONAL = 10.0


@dataclass(frozen=True)
class SignalParams:
    high_vol_atr_pct: float = 0.018
    high_vol_bb_width: float = 0.12
    trend_rsi_min: float = 50.0
    trend_rsi_max: float = 70.0
    trend_vol_min: float = 1.0
    breakout_close_above_high_pct: float = 0.003
    breakout_rsi_min: float = 58.0
    breakout_vol_min: float = 1.35
    range_bb_width_max: float = 0.045
    range_trend_strength_max: float = 0.003
    range_rsi_max: float = 30.0
    range_vol_max: float = 1.05
    range_touch_pct: float = 0.001
    chop_rsi_max: float = 28.0
    chop_vol_min: float = 1.0
    sl_floor: float = 0.004
    sl_cap: float = 0.028
    tp1_floor: float = 0.010
    tp1_cap: float = 0.040
    tp2_floor: float = 0.018
    tp2_cap: float = 0.085
    tp3_floor: float = 0.030
    tp3_cap: float = 0.120
    sl_atr_mult: float = 1.0
    tp1_atr_mult: float = 2.0
    tp2_atr_mult: float = 3.0
    tp3_atr_mult: float = 6.0
    trail_atr_mult: float = 1.0
    trend_size_multiplier: float = 1.15
    breakout_size_multiplier: float = 1.00
    range_size_multiplier: float = 0.85
    chop_size_multiplier: float = 0.70


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float = 10_000.0
    maker_fee_bps: float = 2.0
    taker_fee_bps: float = 6.0
    slippage_bps: float = 3.0
    slippage_atr_mult: float = 0.10
    latency_bars: int = 1
    warmup_bars: int = 250
    max_positions: int = 3
    timeframe: str = "1h"
    symbols: Tuple[str, ...] = tuple(DEFAULT_SYMBOLS)
    seed: int = 42


@dataclass
class Position:
    symbol: str
    entry_idx: int
    entry_ts: pd.Timestamp
    entry: float
    qty: float
    original_qty: float
    entry_fee_remaining: float
    strategy: str
    regime: str
    sl: float
    tp1: float
    tp2: float
    tp3: float
    trail_pct: float
    tp1_frac: float
    tp2_frac: float
    tp3_frac: float
    confidence: float
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    high: float = 0.0


@dataclass
class Result:
    params: SignalParams
    config: BacktestConfig
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    profit_factor: float
    total_fees: float
    total_slippage: float
    total_pnl: float
    final_equity: float
    return_pct: float
    max_drawdown_pct: float
    sharpe: float
    calmar: float
    expectancy: float
    avg_trade: float
    by_symbol: Dict[str, float]
    by_strategy: Dict[str, float]
    equity_curve: List[Tuple[str, float]]
    trades: List[Dict]

    def to_dict(self):
        d = asdict(self)
        d["params"] = asdict(self.params)
        d["config"] = asdict(self.config)
        return d


def utc(x):
    ts = pd.Timestamp(x)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def fee_rate(cfg: BacktestConfig, kind: str) -> float:
    return (cfg.maker_fee_bps if kind == "maker" else cfg.taker_fee_bps) / 10000.0


def load_history(symbol: str, timeframe: str, start: str, end: str, cache: bool = True) -> pd.DataFrame:
    fn = DATA_DIR / f"{symbol.replace('/', '_')}_{timeframe}_{start}_{end}.csv"
    if cache and fn.exists():
        df = pd.read_csv(fn)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    ex = ccxt.binance({"enableRateLimit": True, "timeout": 20000})
    since = int(utc(start).timestamp() * 1000)
    end_ms = int(utc(end).timestamp() * 1000)
    bars = []

    while since < end_ms:
        chunk = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1000)
        if not chunk:
            break
        bars.extend(chunk)
        since = chunk[-1][0] + 1
        if len(chunk) < 1000:
            break

    df = pd.DataFrame(bars, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = (
        df[(df["timestamp"] >= utc(start)) & (df["timestamp"] <= utc(end))]
        .drop_duplicates("timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    fn.parent.mkdir(exist_ok=True)
    df.to_csv(fn, index=False)
    return df


def _safe_indicator_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee all fields used by signal() exist, even if strategy.py is older.

    This keeps backtest and live logic compatible across repo versions.
    """
    df = df.copy()

    if "high_20" not in df.columns:
        df["high_20"] = df["high"].rolling(20).max()
    if "low_20" not in df.columns:
        df["low_20"] = df["low"].rolling(20).min()
    if "trend_strength" not in df.columns:
        if "ema20" in df.columns and "ema50" in df.columns:
            df["trend_strength"] = (df["ema20"] - df["ema50"]).abs() / df["close"]
        else:
            df["trend_strength"] = np.nan

    return df


def prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if compute_indicators:
        df = compute_indicators(df)
    else:
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
        d = df["close"].diff()
        g = d.clip(lower=0).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        l = (-d.clip(upper=0)).ewm(alpha=1 / 14, min_periods=14, adjust=False).mean().replace(0, pd.NA)
        df["rsi"] = 100 - 100 / (1 + g / l)
        pc = df["close"].shift(1)
        tr = pd.concat(
            [(df["high"] - df["low"]), (df["high"] - pc).abs(), (df["low"] - pc).abs()],
            axis=1,
        ).max(axis=1)
        df["atr"] = tr.rolling(14).mean()
        df["atr_pct"] = df["atr"] / df["close"]
        df["vol_ratio"] = df["volume"] / df["volume"].rolling(20).mean()
        mid = df["close"].rolling(20).mean()
        std = df["close"].rolling(20).std()
        df["bb_upper"] = mid + 2 * std
        df["bb_lower"] = mid - 2 * std
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / mid
        df["high_20"] = df["high"].rolling(20).max()
        df["low_20"] = df["low"].rolling(20).min()
        df["trend_strength"] = (df["ema20"] - df["ema50"]).abs() / df["close"]

    df = _safe_indicator_columns(df)
    return df.dropna().reset_index(drop=True)


def tp(atr_pct, floor, mult, cap):
    return max(floor, min(cap, max(floor, atr_pct * mult)))


def signal(symbol: str, df: pd.DataFrame, p: SignalParams):
    if len(df) < 50:
        return None

    r = df.iloc[-1]
    if r["atr_pct"] > p.high_vol_atr_pct or r["bb_width"] > p.high_vol_bb_width:
        return None

    reg = "chop"
    if r["close"] >= r["high_20"] * (1 + p.breakout_close_above_high_pct) and r["vol_ratio"] >= p.breakout_vol_min:
        reg = "breakout"
    elif (r["ema20"] > r["ema50"] > r["ema200"]) or (r["ema20"] < r["ema50"] < r["ema200"]):
        reg = "trend"
    elif r["bb_width"] < p.range_bb_width_max and r["trend_strength"] < p.range_trend_strength_max:
        reg = "range"

    a = float(r["atr_pct"])
    if reg == "trend" and r["ema20"] > r["ema50"] > r["ema200"] and p.trend_rsi_min <= r["rsi"] <= p.trend_rsi_max and r["vol_ratio"] >= p.trend_vol_min:
        return dict(
            side="LONG",
            strategy="trend_follow",
            regime=reg,
            confidence=0.75,
            stop_loss_pct=tp(a, p.sl_floor, p.sl_atr_mult, p.sl_cap),
            take_profit_pct=tp(a, p.tp1_floor, p.tp1_atr_mult, p.tp1_cap),
            secondary_take_profit_pct=tp(a, p.tp2_floor, p.tp2_atr_mult, p.tp2_cap),
            tp3_pct=tp(a, p.tp3_floor, p.tp3_atr_mult, p.tp3_cap),
            trail_pct=tp(a, 0.007, p.trail_atr_mult, 0.03),
            tp1_close_fraction=0.35,
            tp2_close_fraction=0.50,
            tp3_close_fraction=0.15,
            size_multiplier=p.trend_size_multiplier,
        )
    if reg == "breakout" and r["close"] >= r["high_20"] * (1 + p.breakout_close_above_high_pct) and r["rsi"] >= p.breakout_rsi_min and r["vol_ratio"] >= p.breakout_vol_min and r["ema20"] >= r["ema50"]:
        return dict(
            side="LONG",
            strategy="breakout_momentum",
            regime=reg,
            confidence=0.70,
            stop_loss_pct=tp(a, p.sl_floor, p.sl_atr_mult * 1.1, p.sl_cap),
            take_profit_pct=tp(a, p.tp1_floor, p.tp1_atr_mult, p.tp1_cap),
            secondary_take_profit_pct=tp(a, p.tp2_floor, p.tp2_atr_mult, p.tp2_cap),
            tp3_pct=tp(a, p.tp3_floor, p.tp3_atr_mult, p.tp3_cap),
            trail_pct=tp(a, 0.006, p.trail_atr_mult * 1.1, 0.028),
            tp1_close_fraction=0.35,
            tp2_close_fraction=0.50,
            tp3_close_fraction=0.20,
            size_multiplier=p.breakout_size_multiplier,
        )
    if reg == "range" and r["close"] <= r["bb_lower"] * (1 + p.range_touch_pct) and r["rsi"] <= p.range_rsi_max and r["vol_ratio"] <= p.range_vol_max:
        return dict(
            side="LONG",
            strategy="range_reversion",
            regime=reg,
            confidence=0.60,
            stop_loss_pct=tp(a, p.sl_floor, 0.9, p.sl_cap * 0.75),
            take_profit_pct=tp(a, p.tp1_floor * 0.8, 1.6, p.tp1_cap * 0.75),
            secondary_take_profit_pct=tp(a, p.tp2_floor * 0.8, 2.1, p.tp2_cap * 0.75),
            tp3_pct=0.0,
            trail_pct=tp(a, 0.004, 0.8, 0.018),
            tp1_close_fraction=0.50,
            tp2_close_fraction=0.50,
            tp3_close_fraction=0.0,
            size_multiplier=p.range_size_multiplier,
        )
    if reg == "chop" and r["rsi"] <= p.chop_rsi_max and r["close"] > r["ema200"] and r["vol_ratio"] >= p.chop_vol_min:
        return dict(
            side="LONG",
            strategy="chop_bounce",
            regime=reg,
            confidence=0.55,
            stop_loss_pct=tp(a, p.sl_floor, 0.8, p.sl_cap * 0.65),
            take_profit_pct=tp(a, p.tp1_floor * 0.7, 1.4, p.tp1_cap * 0.65),
            secondary_take_profit_pct=tp(a, p.tp2_floor * 0.7, 1.9, p.tp2_cap * 0.65),
            tp3_pct=tp(a, p.tp3_floor, 4.0, p.tp3_cap * 0.75),
            trail_pct=tp(a, 0.0035, 0.7, 0.015),
            tp1_close_fraction=0.50,
            tp2_close_fraction=0.50,
            tp3_close_fraction=0.10,
            size_multiplier=p.chop_size_multiplier,
        )
    return None


def slip_rate(bar, cfg: BacktestConfig):
    vol = max(0.5, min(3.0, float(bar.get("vol_ratio", 1.0) or 1.0)))
    atr_pct = float(bar.get("atr_pct", 0.0) or 0.0)
    return max(0.0, ((cfg.slippage_bps / 10000.0) + atr_pct * cfg.slippage_atr_mult) / vol)


def bars_per_year(tf):
    if tf.endswith("m"):
        return int((365 * 24 * 60) / int(tf[:-1]))
    if tf.endswith("h"):
        return int((365 * 24) / int(tf[:-1]))
    return 365


class Engine:
    def __init__(self, cfg: BacktestConfig, p: SignalParams):
        self.cfg, self.p = cfg, p
        self.cash = cfg.initial_capital
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, List[Tuple[int, dict]]] = {}
        self.trades: List[dict] = []
        self.equity: List[Tuple[str, float]] = []
        self.total_fees = 0.0
        self.total_slip = 0.0

    def _open(self, sym, idx, bar, sig, equity):
        if len(self.positions) >= self.cfg.max_positions or sym in self.positions:
            return

        open_price = float(bar["open"])
        entry_slip = slip_rate(bar, self.cfg)
        entry_price = open_price * (1 + entry_slip)
        entry_fee_rate = fee_rate(self.cfg, "taker")

        if calculate_position:
            target_qty, _ = calculate_position(
                sym,
                entry_price,
                equity,
                float(sig["stop_loss_pct"]),
                float(sig["confidence"]),
                1.0,
                float(sig["size_multiplier"]),
            )
        else:
            target_qty = equity * 0.33 / entry_price

        max_affordable_qty = self.cash / (entry_price * (1 + entry_fee_rate)) if entry_price > 0 else 0.0
        qty = max(0.0, min(float(target_qty or 0.0), max_affordable_qty))
        notional = qty * entry_price
        entry_fee = notional * entry_fee_rate

        if qty <= 0 or notional < MIN_NOTIONAL:
            return
        if notional + entry_fee > self.cash:
            qty = self.cash / (entry_price * (1 + entry_fee_rate))
            notional = qty * entry_price
            entry_fee = notional * entry_fee_rate

        if qty <= 0 or notional < MIN_NOTIONAL:
            return

        self.cash -= (notional + entry_fee)
        self.total_fees += entry_fee
        self.total_slip += abs(entry_price - open_price) * qty

        self.positions[sym] = Position(
            symbol=sym,
            entry_idx=idx,
            entry_ts=bar.name,
            entry=entry_price,
            qty=qty,
            original_qty=qty,
            entry_fee_remaining=entry_fee,
            strategy=sig["strategy"],
            regime=sig["regime"],
            sl=entry_price * (1 - sig["stop_loss_pct"]),
            tp1=entry_price * (1 + sig["take_profit_pct"]),
            tp2=entry_price * (1 + sig["secondary_take_profit_pct"]),
            tp3=entry_price * (1 + float(sig["tp3_pct"] or 0.0)),
            trail_pct=float(sig["trail_pct"]),
            tp1_frac=float(sig["tp1_close_fraction"]),
            tp2_frac=float(sig["tp2_close_fraction"]),
            tp3_frac=float(sig["tp3_close_fraction"]),
            confidence=float(sig["confidence"]),
            high=entry_price,
        )
        self.trades.append(
            {
                "type": "entry",
                "timestamp": str(bar.name),
                "symbol": sym,
                "price": entry_price,
                "qty": qty,
                "fee": entry_fee,
                "strategy": sig["strategy"],
            }
        )

    def _close(self, sym, bar, pos, trigger_price, qty, reason, kind):
        qty = min(float(qty), float(pos.qty))
        if qty <= 0:
            return

        open_price = float(bar["open"])
        slip = slip_rate(bar, self.cfg) * (0.8 if kind != "sl" else 1.0)
        if reason == "stop_loss":
            raw_fill = min(open_price, trigger_price)
        elif reason in {"tp1", "tp2", "tp3", "eod"}:
            raw_fill = max(open_price, trigger_price)
        else:
            raw_fill = trigger_price

        fill = raw_fill * (1 - slip)
        exit_fee = fill * qty * fee_rate(self.cfg, "maker" if kind == "tp" else "taker")

        entry_fee_alloc = 0.0
        if pos.original_qty > 0 and pos.entry_fee_remaining > 0:
            share = qty / pos.qty if pos.qty > 0 else 1.0
            entry_fee_alloc = min(pos.entry_fee_remaining, pos.entry_fee_remaining * share)
            pos.entry_fee_remaining = max(0.0, pos.entry_fee_remaining - entry_fee_alloc)

        gross_pnl = (fill - pos.entry) * qty
        net_pnl = gross_pnl - exit_fee - entry_fee_alloc

        self.cash += (fill * qty) - exit_fee
        self.total_fees += exit_fee
        self.total_slip += abs(fill - raw_fill) * qty
        self.trades.append(
            {
                "type": "exit",
                "timestamp": str(bar.name),
                "symbol": sym,
                "price": fill,
                "qty": qty,
                "fee": exit_fee + entry_fee_alloc,
                "entry_fee_alloc": entry_fee_alloc,
                "gross_pnl": gross_pnl,
                "pnl": net_pnl,
                "reason": reason,
                "strategy": pos.strategy,
            }
        )
        pos.qty -= qty

    def _manage(self, sym, bar):
        pos = self.positions.get(sym)
        if not pos:
            return

        h = float(bar["high"])
        l = float(bar["low"])
        o = float(bar["open"])
        pos.high = max(pos.high, h)

        if l <= pos.sl:
            self._close(sym, bar, pos, pos.sl, pos.qty, "stop_loss", "sl")
            if pos.qty <= 1e-12:
                self.positions.pop(sym, None)
            return

        if pos.tp1_hit and pos.trail_pct > 0:
            pos.sl = max(pos.sl, h * (1 - pos.trail_pct))

        if not pos.tp1_hit and h >= pos.tp1:
            q = min(pos.original_qty * pos.tp1_frac, pos.qty)
            self._close(sym, bar, pos, pos.tp1, q, "tp1", "tp")
            pos.tp1_hit = True
            pos.sl = max(pos.sl, pos.entry)

        if sym in self.positions:
            p2 = self.positions[sym]
            if not p2.tp2_hit and h >= p2.tp2:
                q = min(p2.original_qty * p2.tp2_frac, p2.qty)
                self._close(sym, bar, p2, p2.tp2, q, "tp2", "tp")
                p2.tp2_hit = True

        if sym in self.positions:
            p3 = self.positions[sym]
            if p3.tp3 > 0 and not p3.tp3_hit and h >= p3.tp3:
                self._close(sym, bar, p3, p3.tp3, p3.qty, "tp3", "tp")
                p3.tp3_hit = True
            if p3.qty <= 1e-12:
                self.positions.pop(sym, None)

    def run(self, data_map: Dict[str, pd.DataFrame]) -> Result:
        frames = {s: prep(df) for s, df in data_map.items()}
        timeline = sorted(set().union(*(set(df["timestamp"]) for df in frames.values())))
        idxmap = {s: {ts: i for i, ts in enumerate(df["timestamp"])} for s, df in frames.items()}
        mmap = {s: df.set_index("timestamp") for s, df in frames.items()}

        for ts in timeline:
            prices = {}

            for s in frames:
                if ts not in mmap[s].index:
                    continue
                bar = mmap[s].loc[ts]
                bar = bar.iloc[-1] if isinstance(bar, pd.DataFrame) else bar
                prices[s] = float(bar["close"])
                if s in self.positions:
                    self._manage(s, bar)

            equity = self.cash + sum(prices.get(s, p.high) * p.qty for s, p in self.positions.items())
            self.equity.append((str(ts), float(equity)))

            for s, df in frames.items():
                if ts not in mmap[s].index:
                    continue
                i = idxmap[s][ts]
                if i < self.cfg.warmup_bars or s in self.positions:
                    continue
                sig = signal(s, df.iloc[: i + 1], self.p)
                if sig:
                    ex = i + max(1, self.cfg.latency_bars)
                    if ex < len(df):
                        self.pending.setdefault(s, []).append((ex, sig))

            for s, q in list(self.pending.items()):
                if s not in frames or not q:
                    continue
                i = idxmap[s].get(ts)
                if i is None:
                    continue
                remain = []
                for ex, sig in q:
                    if ex == i and s not in self.positions:
                        self._open(s, i, frames[s].iloc[i], sig, equity)
                    else:
                        remain.append((ex, sig))
                self.pending[s] = remain

        for s, p in list(self.positions.items()):
            last = frames[s].iloc[-1]
            self._close(s, last, p, float(last["close"]), p.qty, "eod", "tp")
            self.positions.pop(s, None)

        return self._result()

    def _result(self) -> Result:
        exits = pd.DataFrame([t for t in self.trades if t["type"] == "exit"])
        pnl = exits["pnl"].astype(float) if not exits.empty and "pnl" in exits else pd.Series(dtype=float)
        wins = int((pnl > 0).sum())
        losses = int((pnl <= 0).sum())
        gross_profit = float(pnl[pnl > 0].sum()) if len(pnl) else 0.0
        gross_loss = float(abs(pnl[pnl < 0].sum())) if len(pnl) else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        eq = [v for _, v in self.equity] or [self.cfg.initial_capital]
        eq_arr = np.array(eq, dtype=float)
        peak = np.maximum.accumulate(eq_arr)
        drawdown = (eq_arr - peak) / np.where(peak == 0, 1, peak)
        final_equity = float(eq_arr[-1])
        return_pct = (final_equity / self.cfg.initial_capital - 1) * 100
        returns = np.diff(eq_arr) / np.where(eq_arr[:-1] == 0, 1, eq_arr[:-1]) if len(eq_arr) > 1 else np.array([0.0])
        sharpe = (
            returns.mean() / returns.std(ddof=1) * math.sqrt(bars_per_year(self.cfg.timeframe))
            if len(returns) > 1 and returns.std(ddof=1) > 0
            else 0.0
        )
        calmar = (return_pct / 100) / abs(drawdown.min()) if drawdown.min() < 0 else 0.0
        expectancy = float(pnl.mean()) if len(pnl) else 0.0
        avg_trade = float(pnl.mean()) if len(pnl) else 0.0

        return Result(
            self.p,
            self.cfg,
            len(exits),
            wins,
            losses,
            wins / len(pnl) if len(pnl) else 0.0,
            gross_profit,
            gross_loss,
            profit_factor,
            self.total_fees,
            self.total_slip,
            final_equity - self.cfg.initial_capital,
            final_equity,
            return_pct,
            float(drawdown.min() * 100),
            float(sharpe),
            float(calmar),
            expectancy,
            avg_trade,
            exits.groupby("symbol")["pnl"].sum().astype(float).to_dict() if not exits.empty else {},
            exits.groupby("strategy")["pnl"].sum().astype(float).to_dict() if not exits.empty else {},
            self.equity,
            self.trades,
        )


def score(r: Result):
    if r.total_trades < 20:
        return -1_000.0 + r.return_pct
    if r.return_pct <= 0 or r.profit_factor < 1.0 or r.win_rate < 0.35:
        return -500.0 + r.return_pct - abs(r.max_drawdown_pct) - (1.0 - min(r.profit_factor, 1.0)) * 100
    return (
        r.return_pct * 2.0
        + r.win_rate * 120.0
        + r.sharpe * 15.0
        + (math.log(max(r.profit_factor, 1.0)) * 50.0)
        - abs(r.max_drawdown_pct) * 3.0
        - (r.total_fees / max(1.0, r.config.initial_capital)) * 100.0
    )


def sample_params(rng):
    return replace(
        SignalParams(),
        trend_rsi_min=rng.uniform(48, 55),
        trend_rsi_max=rng.uniform(66, 76),
        trend_vol_min=rng.uniform(0.95, 1.10),
        breakout_close_above_high_pct=rng.uniform(0.0015, 0.0050),
        breakout_rsi_min=rng.uniform(55, 62),
        breakout_vol_min=rng.uniform(1.20, 1.60),
        range_bb_width_max=rng.uniform(0.035, 0.055),
        range_trend_strength_max=rng.uniform(0.0025, 0.0040),
        range_rsi_max=rng.uniform(26, 34),
        range_vol_max=rng.uniform(0.95, 1.10),
        range_touch_pct=rng.uniform(0.0005, 0.0020),
        chop_rsi_max=rng.uniform(24, 30),
        chop_vol_min=rng.uniform(0.90, 1.10),
        sl_atr_mult=rng.uniform(0.8, 1.3),
        tp1_atr_mult=rng.uniform(1.5, 2.8),
        tp2_atr_mult=rng.uniform(2.5, 4.5),
        tp3_atr_mult=rng.uniform(4.5, 7.5),
        trail_atr_mult=rng.uniform(0.7, 1.5),
        trend_size_multiplier=rng.uniform(1.0, 1.25),
        breakout_size_multiplier=rng.uniform(0.85, 1.05),
        range_size_multiplier=rng.uniform(0.70, 1.0),
        chop_size_multiplier=rng.uniform(0.55, 0.90),
    )


def optimize(data_map, cfg, trials=75, seed=42):
    rng = random.Random(seed)
    best = None
    board = []
    for i in range(trials):
        p = sample_params(rng)
        r = Engine(cfg, p).run(data_map)
        s = score(r)
        board.append(
            {
                "trial": i + 1,
                "score": s,
                "return_pct": r.return_pct,
                "max_dd_pct": r.max_drawdown_pct,
                "win_rate": r.win_rate,
                "pf": r.profit_factor,
                "trades": r.total_trades,
                "params": asdict(p),
            }
        )
        if best is None or s > score(best[1]):
            best = (p, r)
    board.sort(key=lambda x: x["score"], reverse=True)
    return best[0], best[1], board


def split_df(df, s1, e1, s2, e2):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return (
        df[(df["timestamp"] >= utc(s1)) & (df["timestamp"] <= utc(e1))].reset_index(drop=True),
        df[(df["timestamp"] >= utc(s2)) & (df["timestamp"] <= utc(e2))].reset_index(drop=True),
    )


def walkforward(data_map, cfg, train_start, train_end, test_start, test_end, trials=75, seed=42):
    train, test = {}, {}
    for s, df in data_map.items():
        train[s], test[s] = split_df(df, train_start, train_end, test_start, test_end)
    p, train_r, board = optimize(train, cfg, trials, seed)
    test_r = Engine(cfg, p).run(test)
    return p, train_r, test_r, board


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backtest", "optimize", "walkforward"], default="walkforward")
    ap.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    ap.add_argument("--timeframe", default="1h")
    ap.add_argument("--start", default="2022-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--train-start", default="2022-01-01")
    ap.add_argument("--train-end", default="2024-12-31")
    ap.add_argument("--test-start", default="2025-01-01")
    ap.add_argument("--test-end", default="2026-12-31")
    ap.add_argument("--capital", type=float, default=10_000.0)
    ap.add_argument("--maker-fee-bps", type=float, default=2.0)
    ap.add_argument("--taker-fee-bps", type=float, default=6.0)
    ap.add_argument("--slippage-bps", type=float, default=3.0)
    ap.add_argument("--slippage-atr-mult", type=float, default=0.10)
    ap.add_argument("--latency-bars", type=int, default=1)
    ap.add_argument("--trials", type=int, default=75)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--save", action="store_true")
    ap.add_argument("--no-cache", action="store_true")
    a = ap.parse_args()

    cfg = BacktestConfig(
        initial_capital=a.capital,
        maker_fee_bps=a.maker_fee_bps,
        taker_fee_bps=a.taker_fee_bps,
        slippage_bps=a.slippage_bps,
        slippage_atr_mult=a.slippage_atr_mult,
        latency_bars=a.latency_bars,
        timeframe=a.timeframe,
        symbols=tuple(a.symbols),
        seed=a.seed,
    )
    data = {
        s: load_history(
            s,
            a.timeframe,
            a.start if a.mode == "backtest" else a.train_start,
            a.end if a.mode == "backtest" else a.test_end,
            cache=not a.no_cache,
        )
        for s in a.symbols
    }

    if a.mode == "backtest":
        r = Engine(cfg, SignalParams()).run(data)
        print(
            json.dumps(
                {
                    "total_trades": r.total_trades,
                    "win_rate": r.win_rate,
                    "total_pnl": r.total_pnl,
                    "return_pct": r.return_pct,
                    "max_dd_pct": r.max_drawdown_pct,
                    "sharpe": r.sharpe,
                    "calmar": r.calmar,
                    "pf": r.profit_factor,
                    "fees": r.total_fees,
                    "slippage": r.total_slippage,
                    "by_symbol": r.by_symbol,
                    "by_strategy": r.by_strategy,
                },
                indent=2,
                default=str,
            )
        )
        if a.save:
            (RESULT_DIR / "backtest_result.json").write_text(json.dumps(r.to_dict(), indent=2, default=str))
    elif a.mode == "optimize":
        train = {
            s: prep(df[(df["timestamp"] >= utc(a.train_start)) & (df["timestamp"] <= utc(a.train_end))].reset_index(drop=True))
            for s, df in data.items()
        }
        p, r, board = optimize(train, cfg, a.trials, a.seed)
        print(
            json.dumps(
                {
                    "best_params": asdict(p),
                    "train": {
                        "total_trades": r.total_trades,
                        "win_rate": r.win_rate,
                        "total_pnl": r.total_pnl,
                        "return_pct": r.return_pct,
                        "max_dd_pct": r.max_drawdown_pct,
                        "pf": r.profit_factor,
                    },
                },
                indent=2,
                default=str,
            )
        )
        if a.save:
            (RESULT_DIR / "optimizer_best_params.json").write_text(json.dumps(asdict(p), indent=2, default=str))
            (RESULT_DIR / "optimizer_train_result.json").write_text(json.dumps(r.to_dict(), indent=2, default=str))
            (RESULT_DIR / "optimizer_leaderboard.json").write_text(json.dumps(board, indent=2, default=str))
    else:
        p, train_r, test_r, board = walkforward(data, cfg, a.train_start, a.train_end, a.test_start, a.test_end, a.trials, a.seed)
        print(
            json.dumps(
                {
                    "best_params": asdict(p),
                    "train": {
                        "total_trades": train_r.total_trades,
                        "win_rate": train_r.win_rate,
                        "total_pnl": train_r.total_pnl,
                        "return_pct": train_r.return_pct,
                        "max_dd_pct": train_r.max_drawdown_pct,
                        "pf": train_r.profit_factor,
                    },
                    "test": {
                        "total_trades": test_r.total_trades,
                        "win_rate": test_r.win_rate,
                        "total_pnl": test_r.total_pnl,
                        "return_pct": test_r.return_pct,
                        "max_dd_pct": test_r.max_drawdown_pct,
                        "pf": test_r.profit_factor,
                    },
                },
                indent=2,
                default=str,
            )
        )
        if a.save:
            (RESULT_DIR / "walkforward_best_params.json").write_text(json.dumps(asdict(p), indent=2, default=str))
            (RESULT_DIR / "walkforward_train_result.json").write_text(json.dumps(train_r.to_dict(), indent=2, default=str))
            (RESULT_DIR / "walkforward_test_result.json").write_text(json.dumps(test_r.to_dict(), indent=2, default=str))
            (RESULT_DIR / "walkforward_leaderboard.json").write_text(json.dumps(board, indent=2, default=str))


if __name__ == "__main__":
    main()
