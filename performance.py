from db import get_conn

def log_trade_performance(strategy, regime, pnl):
    conn = get_conn()
    cur = conn.cursor()

    is_win = pnl > 0

    cur.execute("""
        INSERT INTO strategy_stats (strategy, regime, trades, wins, total_pnl)
        VALUES (%s, %s, 1, %s, %s)
        ON CONFLICT (strategy, regime)
        DO UPDATE SET
            trades = strategy_stats.trades + 1,
            wins = strategy_stats.wins + %s,
            total_pnl = strategy_stats.total_pnl + %s,
            last_updated = NOW()
    """, (
        strategy,
        regime,
        1 if is_win else 0,
        pnl,
        1 if is_win else 0,
        pnl
    ))

    conn.commit()
    conn.close()