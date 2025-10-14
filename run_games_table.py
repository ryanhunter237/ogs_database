import os
import psycopg2
from psycopg2.extras import execute_values
from functools import partial


from load import get_gamedata, game_filter

INSERT_BATCH_SIZE = 5_000

RowData = tuple[int, int, int, int, bool]

dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/mydb")


def create_games_table():
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS games (
                game_id INTEGER PRIMARY KEY,
                start_time INTEGER NOT NULL,
                black_player_id INTEGER NOT NULL,
                white_player_id INTEGER NOT NULL,
                winner INTEGER NOT NULL,
                ranked BOOLEAN NOT NULL
            );
            """
            cur.execute(create_table_sql)


def insert_batch(conn, data: list[RowData]):
    with conn.cursor() as cur:
        insert_sql = """
        INSERT INTO games (game_id, start_time, black_player_id, white_player_id, winner, ranked)
        VALUES %s
        """
        execute_values(cur, insert_sql, data)
    conn.commit()


def run():
    start = 0
    stop = 60_000_000
    print(f"Processing {start}:{stop}")
    create_games_table()

    conn = psycopg2.connect(dsn)
    gf = partial(game_filter, size=19, min_moves=20)
    batch = []
    for gamedata in get_gamedata(start, stop, gf):
        row = (
            gamedata["game_id"],
            gamedata["start_time"],
            gamedata["black_player_id"],
            gamedata["white_player_id"],
            gamedata["winner"],
            bool(gamedata["ranked"]),
        )
        batch.append(row)
        if len(batch) == INSERT_BATCH_SIZE:
            insert_batch(conn, batch)
            batch = []
    if batch:
        insert_batch(conn, batch)
    conn.close()


if __name__ == "__main__":
    run()
