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
