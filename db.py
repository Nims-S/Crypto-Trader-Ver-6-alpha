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
    CREATE TABLE IF NOT EXISTS trades (
        id SERIAL PRIMARY KEY,
        symbol TEXT,
        entry FLOAT,
        exit FLOAT,
        pnl FLOAT,
        regime TEXT DEFAULT 'unknown',
        reason TEXT DEFAULT '',
        confidence FLOAT DEFAULT 0,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    migration_columns = [
        ("positions", "regime", "TEXT DEFAULT 'unknown'"),
        ("positions", "confidence", "FLOAT DEFAULT 0"),
        ("positions", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("trades", "regime", "TEXT DEFAULT 'unknown'"),
        ("trades", "reason", "TEXT DEFAULT ''"),
        ("trades", "confidence", "FLOAT DEFAULT 0"),
    ]

    for table, col_name, col_type in migration_columns:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {col_type}")

    conn.commit()
    conn.close()
    print("✅ Database schema ready for v6 alpha", flush=True)
