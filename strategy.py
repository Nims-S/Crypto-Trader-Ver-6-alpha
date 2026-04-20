from dataclasses import dataclass
from enum import Enum
import pandas as pd

# ===== HARD CONTROL =====
ALLOWED_REGIMES = {"trend"}  # ONLY trade trend


class Regime(str, Enum):
    TREND    = "trend"
    RANGE    = "range"
    BREAKOUT = "breakout"
    HIGH_VOL = "high_vol"
    CHOP     = "chop"


@dataclass
class TradeSignal:
    symbol: str
    side: str
    strategy: str
    regime: str
    confidence: float
    reason: str
    stop_loss_pct: float
    take_profit_pct: float
    secondary_take_profit_pct: float
    trail_pct: float
    trail_atr_mult: float = 0.0
    size_multiplier: float = 1.0
    tp1_close_fraction: float = 0.0
    tp2_close_fraction: float = 0.0
    tp3_pct: float = 0.0
    tp3_enabled: bool = False
    tp3_close_fraction: float = 0.0


def _cap(value, low, high):
    return max(low, min(high, float(value)))


def _calc_tp(atr_pct, floor, mult, cap):
    return _cap(max(floor, float(atr_pct) * mult), floor, cap)


def _volatility_size_multiplier(atr_pct, base, floor, ceiling):
    atr_pct = max(1e-6, float(atr_pct))
    vol_scale = _cap(0.02 / atr_pct, floor, ceiling)
    return _cap(base * vol_scale, 0.5, 1.2)


def _trail_atr_multiplier(symbol: str):
    if symbol == "BTC/USDT":
        return 1.2
    if symbol == "ETH/USDT":
        return 1.25
    if symbol == "SOL/USDT":
        return 1.35
    return 1.2


def no_trade_signal(symbol, regime, reason="No valid setup"):
    return TradeSignal(
        symbol=symbol,
        side="FLAT",
        strategy="no_trade",
        regime=str(regime),
        confidence=0.0,
        reason=reason,
        stop_loss_pct=0,
        take_profit_pct=0,
        secondary_take_profit_pct=0,
        trail_pct=0,
    )


# ================= INDICATORS =================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    df["ema20"]  = df["close"].ewm(span=20).mean()
    df["ema50"]  = df["close"].ewm(span=50).mean()
    df["ema200"] = df["close"].ewm(span=200).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/14).mean()
    avg_loss = loss.ewm(alpha=1/14).mean().replace(0, pd.NA)

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs()
    ], axis=1).max(axis=1)

    df["atr"] = tr.rolling(14).mean()
    df["atr_pct"] = df["atr"] / df["close"]

    df["vol_avg"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg"]

    mid = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    df["bb_width"] = ((mid + 2*std) - (mid - 2*std)) / mid

    df["high_20"] = df["high"].rolling(20).max()

    return df.dropna()


# ================= REGIME =================
def detect_regime(row):
    if row["atr_pct"] > 0.018 or row["bb_width"] > 0.12:
        return Regime.HIGH_VOL

    if row["close"] >= row["high_20"] * 1.002 and row["vol_ratio"] > 1.3:
        return Regime.BREAKOUT

    if row["ema20"] > row["ema50"] > row["ema200"]:
        return Regime.TREND

    if row["bb_width"] < 0.045:
        return Regime.RANGE

    return Regime.CHOP


# ================= SIGNAL =================
def generate_signal(symbol: str, df: pd.DataFrame):
    if df.empty:
        return no_trade_signal(symbol, "unknown", "Empty")

    row = df.iloc[-1]
    regime = detect_regime(row)

    # HARD BLOCK
    if regime.value not in ALLOWED_REGIMES:
        return no_trade_signal(symbol, regime, "Regime blocked")

    atr = float(row["atr_pct"])
    trail_atr = _trail_atr_multiplier(symbol)

    # ===== STRICT TREND FILTER =====
    trend_valid = (
        row["ema20"] > row["ema50"] > row["ema200"]
        and row["close"] > row["ema20"]
        and 55 <= row["rsi"] <= 68
        and row["vol_ratio"] >= 1.2
        and 0.008 <= atr <= 0.025
        and row["bb_width"] >= 0.03
    )

    if not trend_valid:
        return no_trade_signal(symbol, regime, "Strict trend filter fail")

    return TradeSignal(
        symbol=symbol,
        side="LONG",
        strategy="trend_strict",
        regime=regime.value,
        confidence=0.85,
        reason="High-quality trend setup",

        stop_loss_pct=_calc_tp(atr, 0.005, 1.1, 0.02),
        take_profit_pct=_calc_tp(atr, 0.012, 2.2, 0.035),
        secondary_take_profit_pct=_calc_tp(atr, 0.020, 3.5, 0.08),
        trail_pct=_calc_tp(atr, 0.006, 1.2, 0.025),

        trail_atr_mult=trail_atr,
        size_multiplier=_volatility_size_multiplier(atr, 1.0, 0.7, 1.1),

        tp1_close_fraction=0.4,
        tp2_close_fraction=0.4,
        tp3_pct=_calc_tp(atr, 0.05, 5.0, 0.12),
        tp3_enabled=True,
        tp3_close_fraction=0.2,
    )
