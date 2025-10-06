from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import psycopg2
from psycopg2.extras import execute_batch


from load import get_gamedata
from gameproc import get_board_hashes_and_moves

INSERT_BATCH_SIZE = 500

dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/mydb")


def create_moves_table():
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS moves (
                game_id SERIAL NOT NULL,
                board_hash BYTEA NOT NULL,
                move_x SMALLINT NOT NULL,
                move_y SMALLINT NOT NULL
            );
            """
            cur.execute(create_table_sql)


def insert_batch(data: list[tuple]):
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_sql = """
            INSERT INTO moves (game_id, board_hash, move_x, move_y)
            VALUES (%s, %s, %s, %s)
            """
            execute_batch(cur, insert_sql, data)


def get_moves_data(gamedata: dict, num_moves: int) -> list[tuple[int, bytes, int, int]]:
    game_id: int = gamedata["game_id"]
    hashes_and_moves = get_board_hashes_and_moves(gamedata, num_moves)
    return [
        (game_id, board_hash, move_x, move_y)
        for board_hash, (move_x, move_y) in hashes_and_moves
    ]


def run():
    create_moves_table()
    with ProcessPoolExecutor(8) as executor:
        futures = [
            executor.submit(get_moves_data, gamedata, 50)
            for gamedata in get_gamedata(100)
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
    run()
