import psycopg2
from config import DB_URL

def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require", connect_timeout=10)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        symbol TEXT PRIMARY KEY,
        entry FLOAT,
        sl FLOAT,
        tp FLOAT,
        size FLOAT,
        regime TEXT DEFAULT 'unknown',
        confidence FLOAT DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS asset_state (
        symbol TEXT PRIMARY KEY,
        regime TEXT,
        strategy TEXT,
        signal JSONB,
        position JSONB,
        updated_at TIMESTAMP
    )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_controls (
            scope TEXT PRIMARY KEY,   -- 'GLOBAL' or a symbol like 'BTC/USDT'
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            flatten_on_disable BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    for scope in ["GLOBAL", "BTC/USDT", "ETH/USDT", "SOL/USDT"]:
        cur.execute("""
            INSERT INTO trade_controls (scope, enabled, flatten_on_disable)
            VALUES (%s, TRUE, FALSE)
            ON CONFLICT (scope) DO NOTHING
        """, (scope,))
    cur.execute("""
    CREATE TABLE IF NOT EXISTS strategy_stats (
        strategy TEXT,
        regime TEXT,
        trades INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        total_pnl FLOAT DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (strategy, regime)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id BIGSERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        entry FLOAT NOT NULL,
        exit FLOAT NOT NULL,
        pnl FLOAT NOT NULL DEFAULT 0,
        regime TEXT DEFAULT 'unknown',
        reason TEXT DEFAULT '',
        confidence FLOAT DEFAULT 0,
        strategy TEXT DEFAULT 'unknown',
        timestamp TIMESTAMP DEFAULT NOW()
    )
    """)
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'LONG'")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp1_hit BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp2_hit BOOLEAN DEFAULT FALSE")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp2 FLOAT")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'unknown'")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS stop_loss_pct FLOAT DEFAULT 0")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_pct FLOAT DEFAULT 0")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS secondary_take_profit_pct FLOAT DEFAULT 0")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS trail_pct FLOAT DEFAULT 0")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp1_close_fraction FLOAT DEFAULT 0.33")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp2_close_fraction FLOAT DEFAULT 0.5")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS original_size NUMERIC")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp3 NUMERIC")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp3_hit BOOLEAN NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS tp3_close_fraction NUMERIC")
    
    migration_columns = [
        ("positions", "regime", "TEXT DEFAULT 'unknown'"),
        ("positions", "confidence", "FLOAT DEFAULT 0"),
        ("positions", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("positions", "tp2", "FLOAT"),
        ("positions", "strategy", "TEXT DEFAULT 'unknown'"),
        ("positions", "stop_loss_pct", "FLOAT DEFAULT 0"),
        ("positions", "take_profit_pct", "FLOAT DEFAULT 0"),
        ("positions", "secondary_take_profit_pct", "FLOAT DEFAULT 0"),
        ("positions", "trail_pct", "FLOAT DEFAULT 0"),
        ("positions", "tp1_close_fraction", "FLOAT DEFAULT 0.33"),
        ("positions", "tp2_close_fraction", "FLOAT DEFAULT 0.5"),
        ("trades", "regime", "TEXT DEFAULT 'unknown'"),
        ("trades", "reason", "TEXT DEFAULT ''"),
        ("trades", "confidence", "FLOAT DEFAULT 0"),
    ]

    for table, col_name, col_type in migration_columns:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}")

    conn.commit()
    conn.close()
    print("✅ Database schema ready for v6 alpha", flush=True)
