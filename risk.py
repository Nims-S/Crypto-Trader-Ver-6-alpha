from config import (
    CAPITAL,
    ALLOCATION,
    RISK,
    MAX_SYMBOL_EXPOSURE_PCT,
    MAX_DAILY_LOSS_PCT,
    MAX_WEEKLY_LOSS_PCT,
)

# Maximum multiplier applied to initial capital based on historical PnL.
# Prevents phantom-capital inflation after a run of wins.
_MAX_CAPITAL_GROWTH = 3.0
_MIN_CAPITAL_GROWTH = 0.5


def get_dynamic_capital(cur, initial_capital: float) -> float:
    """
    Returns the current effective capital, capped to a safe growth band.

    Calculation: initial_capital × clamp(1 + total_pnl/initial_capital, 0.5, 3.0)

    This prevents position sizes from ballooning on a lucky streak, while
    still reducing exposure after a drawdown.
    """
    cur.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades")
    total_pnl = float(cur.fetchone()[0] or 0.0)
    if initial_capital <= 0:
        return initial_capital
    growth = 1.0 + total_pnl / initial_capital
    growth = max(_MIN_CAPITAL_GROWTH, min(_MAX_CAPITAL_GROWTH, growth))
    return initial_capital * growth


def get_open_exposure(cur) -> float:
    cur.execute("SELECT COALESCE(SUM(entry * size), 0) FROM positions")
    return float(cur.fetchone()[0] or 0.0)


def get_position_count(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM positions")
    return int(cur.fetchone()[0] or 0)


def risk_gate(cur, total_capital: float):
    """Hard portfolio-level guardrails."""
    cur.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades "
        "WHERE timestamp >= NOW() - INTERVAL '1 day'"
    )
    day_pnl = float(cur.fetchone()[0] or 0.0)

    cur.execute(
        "SELECT COALESCE(SUM(pnl), 0) FROM trades "
        "WHERE timestamp >= NOW() - INTERVAL '7 days'"
    )
    week_pnl = float(cur.fetchone()[0] or 0.0)

    if total_capital <= 0:
        return False, "Invalid capital"
    if day_pnl <= -(total_capital * MAX_DAILY_LOSS_PCT):
        return False, "Daily loss limit reached"
    if week_pnl <= -(total_capital * MAX_WEEKLY_LOSS_PCT):
        return False, "Weekly loss limit reached"
    return True, "OK"


def get_strategy_multiplier(cur, strategy: str, regime: str) -> float:
    try:
        cur.execute(
            "SELECT trades, wins, total_pnl FROM strategy_stats "
            "WHERE strategy=%s AND regime=%s",
            (strategy, regime),
        )
        row = cur.fetchone()
        if not row:
            return 1.0
        trades, wins, pnl = row
        if trades < 5:
            return 1.0
        win_rate = wins / trades
        if win_rate > 0.6 and pnl > 0:
            return 1.25
        elif win_rate < 0.4:
            return 0.6
        return 1.0
    except Exception:
        return 1.0


def calculate_position(
    symbol: str,
    price: float,
    total_cap: float,
    stop_loss_pct: float = 0.005,
    confidence: float = 0.5,
    regime_multiplier: float = 1.0,
    size_multiplier: float = 1.0,
):
    """
    Size using the stricter of:
      1) allocation cap
      2) fixed risk-per-trade budget
    """
    ratio        = ALLOCATION.get(symbol, 0.33)
    notional_cap = total_cap * ratio

    confidence_multiplier = max(0.5, min(1.25, confidence))
    regime_multiplier     = max(0.5, min(1.5, float(regime_multiplier or 1.0)))
    size_multiplier       = max(0.5, min(1.25, float(size_multiplier or 1.0)))

    risk_budget      = total_cap * RISK * confidence_multiplier * regime_multiplier * size_multiplier
    stop_distance    = max(price * float(stop_loss_pct), price * 0.0025)
    risk_based_size  = risk_budget / stop_distance
    allocation_size  = notional_cap / price

    size              = max(0.0, min(risk_based_size, allocation_size))
    deployed_capital  = size * price

    # Per-symbol hard cap
    max_symbol_cap = total_cap * MAX_SYMBOL_EXPOSURE_PCT
    if deployed_capital > max_symbol_cap:
        deployed_capital = max_symbol_cap
        size             = deployed_capital / price

    return size, deployed_capital
