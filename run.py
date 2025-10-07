from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import psycopg2
from psycopg2.extras import execute_values
from time import time


from load import get_gamedata
from gameproc import get_board_hashes_and_moves

INSERT_BATCH_SIZE = 5_000

dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/mydb")


def create_moves_table():
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS moves (
                game_id INTEGER NOT NULL,
                move_num SMALLINT NOT NULL,
                board_hash BYTEA NOT NULL,
                move_x SMALLINT NOT NULL,
                move_y SMALLINT NOT NULL,
                UNIQUE (game_id, move_num)
            );
            """
            cur.execute(create_table_sql)


def insert_batch(data: list[tuple]):
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_sql = """
            INSERT INTO moves (game_id, move_num, board_hash, move_x, move_y)
            VALUES %s
            """
            execute_values(cur, insert_sql, data)


def get_moves_data(gamedata: dict, num_moves: int) -> list[tuple[int, int, bytes, int, int]]:
    game_id: int = gamedata["game_id"]
    hashes_and_moves = get_board_hashes_and_moves(gamedata, num_moves)
    data = []
    for i, hash_and_move in enumerate(hashes_and_moves, start=1):
        board_hash, (move_x, move_y) = hash_and_move
        data.append((game_id, i, board_hash, move_x, move_y))
    return data


def run():
    start = 400_000
    stop = 500_000
    print(f"Processing {start}:{stop}")
    create_moves_table()
    with ProcessPoolExecutor(8) as executor:
        futures = [
            executor.submit(get_moves_data, gamedata, 50)
            for gamedata in get_gamedata(start, stop)
        ]

        batch = []
        for future in as_completed(futures):
            results = future.result()
            batch.extend(results)
            if len(batch) >= INSERT_BATCH_SIZE:
                insert_batch(batch)
                batch = []
        if batch:
            insert_batch(batch)

if __name__ == "__main__":
    t0 = time()
    run()
    print(f"{time() - t0:.2f}")
