import os
import psycopg2


def create_move_counts():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise EnvironmentError("DATABASE_URL environment variable not set")

    create_table_sql = f"""
    CREATE TABLE move_counts AS
    SELECT board_hash, move_x, move_y, COUNT(*) AS freq
    FROM moves
    GROUP BY board_hash, move_x, move_y;
    """

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            print("Applying performance tuning parameters...")
            cur.execute("SET synchronous_commit = OFF;")
            cur.execute("SET max_parallel_workers_per_gather = 8;")
            cur.execute("SET parallel_setup_cost = 0;")
            cur.execute("SET parallel_tuple_cost = 0;")
            cur.execute("SET work_mem = '256MB';")

            print("Creating move_counts table (this may take a while)...")
            cur.execute(create_table_sql)

            print("Creating index on move_counts...")
            cur.execute("""
                CREATE INDEX idx_move_counts_hash
                ON move_counts (board_hash);
            """)

            print("✅ move_counts table successfully created and indexed.")

if __name__ == "__main__":
    create_move_counts()
