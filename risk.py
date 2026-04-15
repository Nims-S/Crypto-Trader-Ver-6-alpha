from config import CAPITAL, ALLOCATION, RISK, MAX_SYMBOL_EXPOSURE_PCT, MAX_DAILY_LOSS_PCT, MAX_WEEKLY_LOSS_PCT

def get_dynamic_capital(cur, initial_capital):
    """Total capital = initial capital + closed trade PnL."""
    cur.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades")
    total_pnl = cur.fetchone()[0] or 0.0
    return initial_capital + float(total_pnl)

def get_open_exposure(cur):
    cur.execute("SELECT COALESCE(SUM(entry * size), 0) FROM positions")
    return float(cur.fetchone()[0] or 0.0)

def get_position_count(cur):
    cur.execute("SELECT COUNT(*) FROM positions")
    return int(cur.fetchone()[0] or 0)

def risk_gate(cur, total_capital):
    """Hard portfolio-level guardrails."""
    cur.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE timestamp >= NOW() - INTERVAL '1 day'")
    day_pnl = float(cur.fetchone()[0] or 0.0)
    cur.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE timestamp >= NOW() - INTERVAL '7 day'")
    week_pnl = float(cur.fetchone()[0] or 0.0)

    if total_capital <= 0:
        return False, "Invalid capital"
    if day_pnl <= -(total_capital * MAX_DAILY_LOSS_PCT):
        return False, "Daily loss limit reached"
    if week_pnl <= -(total_capital * MAX_WEEKLY_LOSS_PCT):
        return False, "Weekly loss limit reached"
    return True, "OK"

def calculate_position(symbol, price, total_cap, stop_loss_pct=0.005, confidence=0.5):
    """
    Size using the stricter of:
    1) allocation cap
    2) fixed risk-per-trade budget
    """
    ratio = ALLOCATION.get(symbol, 0.33)
    notional_cap = total_cap * ratio
    risk_budget = total_cap * RISK * max(0.5, min(1.25, confidence))

    stop_distance = max(price * float(stop_loss_pct), price * 0.0025)
    risk_based_size = risk_budget / stop_distance
    allocation_based_size = notional_cap / price

    size = max(0.0, min(risk_based_size, allocation_based_size))
    deployed_capital = size * price

    # Symbol exposure limit
    max_symbol_cap = total_cap * MAX_SYMBOL_EXPOSURE_PCT
    if deployed_capital > max_symbol_cap:
        deployed_capital = max_symbol_cap
        size = deployed_capital / price

    return size, deployed_capital
