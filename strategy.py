from dataclasses import dataclass
from enum import Enum
import pandas as pd

class Regime(str, Enum):
    TREND = "trend"
    RANGE = "range"
    BREAKOUT = "breakout"
    HIGH_VOL = "high_vol"
    CHOP = "chop"

@dataclass
class TradeSignal:
    symbol: str
    side: str              # "LONG" or "FLAT"
    strategy: str
    regime: str
    confidence: float
    reason: str
    stop_loss_pct: float
    take_profit_pct: float
    secondary_take_profit_pct: float
    trail_pct: float
    size_multiplier: float
    tp1_close_fraction: float
    tp2_close_fraction: float

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean().replace(0, pd.NA)
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()
    df["atr_pct"] = df["atr"] / df["close"]

    df["vol_avg"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg"]

    mid = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    df["bb_upper"] = mid + 2 * std
    df["bb_lower"] = mid - 2 * std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / mid

    df["high_20"] = df["high"].rolling(20).max()
    df["low_20"] = df["low"].rolling(20).min()
    df["trend_strength"] = (df["ema20"] - df["ema50"]).abs() / df["close"]

    return df.dropna()

def detect_regime(row) -> Regime:
    trend_up = row["ema20"] > row["ema50"] > row["ema200"]
    trend_down = row["ema20"] < row["ema50"] < row["ema200"]
    high_vol = row["atr_pct"] > 0.018 or row["bb_width"] > 0.12
    breakout = row["close"] >= row["high_20"] * 0.998 and row["vol_ratio"] > 1.2
    range_market = row["bb_width"] < 0.05 and row["trend_strength"] < 0.0035

    if high_vol:
        return Regime.HIGH_VOL
    if breakout:
        return Regime.BREAKOUT
    if trend_up or trend_down:
        return Regime.TREND
    if range_market:
        return Regime.RANGE
    return Regime.CHOP

def generate_signal(symbol: str, df: pd.DataFrame):
    if df.empty:
        return None

    row = df.iloc[-1]
    regime = detect_regime(row)

    # Risk-first: long only, because the current live stack is spot-oriented.
    if regime == Regime.HIGH_VOL:
        return None

    # Trend-following: buy only when momentum agrees.
    if regime == Regime.TREND:
        if row["ema20"] > row["ema50"] > row["ema200"] and 48 <= row["rsi"] <= 72 and row["vol_ratio"] >= 1.0:
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="trend_follow",
                regime=regime.value,
                confidence=0.78,
                reason="Trend alignment with volume confirmation",
                stop_loss_pct=max(0.0045, float(row["atr_pct"]) * 1.2),
                take_profit_pct=max(0.010, float(row["atr_pct"]) * 2.2),
                secondary_take_profit_pct=max(0.018, float(row["atr_pct"]) * 3.4),
                trail_pct=max(0.007, float(row["atr_pct"]) * 1.4),
                size_multiplier=1.15,
                tp1_close_fraction=0.35,
                tp2_close_fraction=0.50,
            )
        return None

    # Breakout: buy momentum expansion after compression.
    if regime == Regime.BREAKOUT:
        if row["close"] >= row["high_20"] * 0.998 and row["rsi"] >= 55 and row["vol_ratio"] >= 1.15:
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="breakout_momentum",
                regime=regime.value,
                confidence=0.73,
                reason="Breakout above 20-bar high with volume expansion",
                stop_loss_pct=max(0.005, float(row["atr_pct"]) * 1.1),
                take_profit_pct=max(0.012, float(row["atr_pct"]) * 2.0),
                secondary_take_profit_pct=max(0.022, float(row["atr_pct"]) * 3.0),
                trail_pct=max(0.006, float(row["atr_pct"]) * 1.1),
                size_multiplier=1.05,
                tp1_close_fraction=0.45,
                tp2_close_fraction=0.60,
            )
        return None

    # Range mean reversion: buy fear near lower band if trend is not broken.
    if regime == Regime.RANGE:
        if row["close"] <= row["bb_lower"] * 1.002 and row["rsi"] <= 34 and row["ema20"] >= row["ema50"] * 0.985:
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="range_reversion",
                regime=regime.value,
                confidence=0.64,
                reason="Range mean reversion at lower band",
                stop_loss_pct=max(0.004, float(row["atr_pct"]) * 0.9),
                take_profit_pct=max(0.008, float(row["atr_pct"]) * 1.6),
                secondary_take_profit_pct=max(0.012, float(row["atr_pct"]) * 2.1),
                trail_pct=max(0.004, float(row["atr_pct"]) * 0.8),
                size_multiplier=0.85,
                tp1_close_fraction=0.50,
                tp2_close_fraction=0.75,
            )
        return None

    # Chop: very selective, only if oversold with decent structure.
    if regime == Regime.CHOP:
        if row["rsi"] <= 30 and row["close"] > row["ema200"] and row["vol_ratio"] > 0.9:
            return TradeSignal(
                symbol=symbol,
                side="LONG",
                strategy="chop_bounce",
                regime=regime.value,
                confidence=0.58,
                reason="Chop filter oversold bounce",
                stop_loss_pct=max(0.004, float(row["atr_pct"]) * 0.8),
                take_profit_pct=max(0.007, float(row["atr_pct"]) * 1.4),
                secondary_take_profit_pct=max(0.010, float(row["atr_pct"]) * 1.9),
                trail_pct=max(0.0035, float(row["atr_pct"]) * 0.7),
                size_multiplier=0.70,
                tp1_close_fraction=0.55,
                tp2_close_fraction=0.80,
            )
        return None

    return None
