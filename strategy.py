from dataclasses import dataclass
from enum import Enum

import pandas as pd


class Regime(str, Enum):
    TREND    = "trend"
    RANGE    = "range"
    BREAKOUT = "breakout"
    HIGH_VOL = "high_vol"
    CHOP     = "chop"


@dataclass
class TradeSignal:
    symbol:                     str
    side:                       str
    strategy:                   str
    regime:                     str
    confidence:                 float
    reason:                     str
    stop_loss_pct:              float
    take_profit_pct:            float
    secondary_take_profit_pct:  float
    trail_pct:                  float
    trail_atr_mult:             float = 0.0
    size_multiplier:            float = 1.0
    tp1_close_fraction:         float = 0.0
    tp2_close_fraction:         float = 0.0
    tp3_pct:                    float = 0.0
    tp3_enabled:                bool  = False
    tp3_close_fraction:         float = 0.0


def _cap(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _calc_tp(atr_pct: float, floor: float, mult: float, cap: float) -> float:
    return _cap(max(floor, float(atr_pct) * mult), floor, cap)


def _volatility_size_multiplier(
    atr_pct: float, base: float, floor: float, ceiling: float
) -> float:
    atr_pct = max(1e-6, float(atr_pct))
    vol_scale = _cap(0.02 / atr_pct, floor, ceiling)
    return _cap(base * vol_scale, 0.45, 1.25)


def _trail_atr_multiplier(symbol: str, regime: Regime) -> float:
    sym = (symbol or "").upper()

    if sym == "BTC/USDT":
        return 1.18 if regime in {Regime.TREND, Regime.BREAKOUT} else 1.10
    if sym == "ETH/USDT":
        return 1.24 if regime in {Regime.TREND, Regime.BREAKOUT} else 1.14
    if sym == "SOL/USDT":
        return 1.34 if regime in {Regime.TREND, Regime.BREAKOUT} else 1.22

    return 1.20 if regime in {Regime.TREND, Regime.BREAKOUT} else 1.10


def no_trade_signal(symbol, regime, reason="No valid setup"):
    return TradeSignal(
        symbol=symbol,
        side="FLAT",
        strategy="no_trade",
        regime=regime.value if hasattr(regime, "value") else str(regime),
        confidence=0.0,
        reason=reason,
        stop_loss_pct=0.0,
        take_profit_pct=0.0,
        secondary_take_profit_pct=0.0,
        trail_pct=0.0,
        trail_atr_mult=0.0,
        size_multiplier=0.0,
        tp1_close_fraction=0.0,
        tp2_close_fraction=0.0,
        tp3_close_fraction=0.0,
    )


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    df["ema20"]  = df["close"].ewm(span=20,  adjust=False).mean()
    df["ema50"]  = df["close"].ewm(span=50,  adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

    delta    = df["close"].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = avg_loss.replace(0, pd.NA)
    rs       = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr"]     = tr.rolling(14).mean()
    df["atr_pct"] = df["atr"] / df["close"]

    df["vol_avg"]   = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg"]

    mid           = df["close"].rolling(20).mean()
    std           = df["close"].rolling(20).std()
    df["bb_upper"] = mid + 2 * std
    df["bb_lower"] = mid - 2 * std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / mid

    df["high_20"]       = df["high"].rolling(20).max()
    df["low_20"]        = df["low"].rolling(20).min()
    df["trend_strength"] = (df["ema20"] - df["ema50"]).abs() / df["close"]

    return df.dropna()


def detect_regime(row) -> Regime:
    trend_up   = row["ema20"] > row["ema50"] > row["ema200"]
    trend_down = row["ema20"] < row["ema50"] < row["ema200"]
    high_vol   = row["atr_pct"] > 0.018 or row["bb_width"] > 0.12
    breakout   = row["close"] >= row["high_20"] * 1.002 and row["vol_ratio"] > 1.3
    range_mkt  = row["bb_width"] < 0.045 and row["trend_strength"] < 0.003

    if high_vol:
        return Regime.HIGH_VOL
    if breakout:
        return Regime.BREAKOUT
    if trend_up or trend_down:
        return Regime.TREND
    if range_mkt:
        return Regime.RANGE
    return Regime.CHOP


def generate_signal(symbol: str, df: pd.DataFrame):
    if df.empty:
        return no_trade_signal(symbol, "unknown", "Empty dataframe")

    row    = df.iloc[-1]
    regime = detect_regime(row)
    atr    = float(row["atr_pct"])
    trail_atr_mult = _trail_atr_multiplier(symbol, regime)

    if regime == Regime.HIGH_VOL:
        return no_trade_signal(symbol, regime, "High volatility filter")

    if regime == Regime.TREND:
        if (
            row["ema20"] > row["ema50"] > row["ema200"]
            and 50 <= row["rsi"] <= 70
            and row["vol_ratio"] >= 1.0
        ):
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="trend_follow",
                regime=regime.value,
                confidence=0.75,
                reason="Trend alignment",
                stop_loss_pct=_calc_tp(atr, 0.0045, 1.2, 0.025),
                take_profit_pct=_calc_tp(atr, 0.010, 2.2, 0.035),
                secondary_take_profit_pct=_calc_tp(atr, 0.018, 3.4, 0.080),
                trail_pct=_calc_tp(atr, 0.007, 1.4, 0.030),
                trail_atr_mult=trail_atr_mult,
                size_multiplier=_volatility_size_multiplier(atr, 1.15, 0.70, 1.10),
                tp1_close_fraction=0.35,
                tp2_close_fraction=0.50,
                tp3_pct=_calc_tp(atr, 0.050, 6.0, 0.120),
                tp3_enabled=True,
                tp3_close_fraction=0.15,
            )
        return no_trade_signal(symbol, regime, "Trend conditions not met")

    if regime == Regime.BREAKOUT:
        if (
            row["close"] >= row["high_20"] * 1.003
            and row["rsi"] >= 58
            and row["vol_ratio"] >= 1.35
            and row["ema20"] >= row["ema50"]
        ):
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="breakout_momentum",
                regime=regime.value,
                confidence=0.7,
                reason="Strong breakout",
                stop_loss_pct=_calc_tp(atr, 0.005, 1.1, 0.028),
                take_profit_pct=_calc_tp(atr, 0.012, 2.0, 0.040),
                secondary_take_profit_pct=_calc_tp(atr, 0.022, 3.0, 0.085),
                trail_pct=_calc_tp(atr, 0.006, 1.1, 0.028),
                trail_atr_mult=trail_atr_mult,
                size_multiplier=_volatility_size_multiplier(atr, 1.0, 0.65, 1.05),
                tp1_close_fraction=0.35,
                tp2_close_fraction=0.50,
                tp3_pct=_calc_tp(atr, 0.060, 7.0, 0.120),
                tp3_enabled=True,
                tp3_close_fraction=0.20,
            )
        return no_trade_signal(symbol, regime, "Breakout conditions not met")

    if regime == Regime.RANGE:
        if (
            row["close"] <= row["bb_lower"] * 0.999
            and row["rsi"] <= 30
            and row["vol_ratio"] <= 1.05
        ):
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="range_reversion",
                regime=regime.value,
                confidence=0.6,
                reason="Tight range bounce",
                stop_loss_pct=_calc_tp(atr, 0.004, 0.9, 0.020),
                take_profit_pct=_calc_tp(atr, 0.008, 1.6, 0.025),
                secondary_take_profit_pct=_calc_tp(atr, 0.012, 2.1, 0.040),
                trail_pct=_calc_tp(atr, 0.004, 0.8, 0.018),
                trail_atr_mult=trail_atr_mult,
                size_multiplier=_volatility_size_multiplier(atr, 0.85, 0.55, 1.00),
                tp1_close_fraction=0.50,
                tp2_close_fraction=0.50,
                tp3_pct=0.0,
                tp3_enabled=False,
                tp3_close_fraction=0.0,
            )
        return no_trade_signal(symbol, regime, "Range conditions not met")

    if regime == Regime.CHOP:
        if (
            row["rsi"] <= 28
            and row["close"] > row["ema200"]
            and row["vol_ratio"] >= 1.0
        ):
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="chop_bounce",
                regime=regime.value,
                confidence=0.55,
                reason="Selective chop bounce",
                stop_loss_pct=_calc_tp(atr, 0.004, 0.8, 0.018),
                take_profit_pct=_calc_tp(atr, 0.007, 1.4, 0.022),
                secondary_take_profit_pct=_calc_tp(atr, 0.010, 1.9, 0.035),
                trail_pct=_calc_tp(atr, 0.0035, 0.7, 0.015),
                trail_atr_mult=trail_atr_mult,
                size_multiplier=_volatility_size_multiplier(atr, 0.70, 0.50, 0.95),
                tp1_close_fraction=0.50,
                tp2_close_fraction=0.50,
                tp3_pct=_calc_tp(atr, 0.030, 4.0, 0.090),
                tp3_enabled=True,
                tp3_close_fraction=0.10,
            )
        return no_trade_signal(symbol, regime, "Chop conditions not met")

    return no_trade_signal(symbol, regime, "No matching regime logic")
