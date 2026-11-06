import duckdb

DB_PATH = "telemetry.duckdb"

def get_connection():
    """Opens a connection to our local DuckDB file, creating it if it doesn't exist."""
    con = duckdb.connect(DB_PATH)
    _create_tables(con)
    return con


def _create_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS clean_events (
            device_id VARCHAR,
            temperature DOUBLE,
            humidity DOUBLE,
            cpu_usage DOUBLE,
            event_timestamp TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS rejected_events (
            device_id VARCHAR,
            temperature DOUBLE,
            humidity DOUBLE,
            cpu_usage DOUBLE,
            event_timestamp TIMESTAMP,
            failure_reason VARCHAR,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def write_clean(con, df):
    """Insert a Polars DataFrame of valid rows into clean_events."""
    con.execute("""
        INSERT INTO clean_events (device_id, temperature, humidity, cpu_usage, event_timestamp)
        SELECT device_id, temperature, humidity, cpu_usage, event_timestamp FROM df
    """)


def write_rejected(con, df):
    """Insert a Polars DataFrame of invalid rows into rejected_events."""
    con.execute("""
        INSERT INTO rejected_events (device_id, temperature, humidity, cpu_usage, event_timestamp, failure_reason)
        SELECT device_id, temperature, humidity, cpu_usage, event_timestamp, failure_reason FROM df
    """)