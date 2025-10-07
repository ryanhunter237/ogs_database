import os
import psycopg2
from psycopg2.extras import execute_values
from time import time, sleep
import multiprocessing as mp


from load import get_gamedata
from gameproc import get_board_hashes_and_moves

NUM_WORKERS = 6
GAMEDATA_BATCH_SIZE = 50
SENTINEL = None
NUM_MOVES = 50
INSERT_BATCH_SIZE = 5_000

RowData = tuple[int, int, bytes, int, int]

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
                move_y SMALLINT NOT NULL
            );
            """
            cur.execute(create_table_sql)


def insert_batch(conn, data: list[RowData]):
    with conn.cursor() as cur:
        insert_sql = """
        INSERT INTO moves (game_id, move_num, board_hash, move_x, move_y)
        VALUES %s
        """
        execute_values(cur, insert_sql, data)
    conn.commit()


def get_moves_data(gamedata: dict, num_moves: int) -> list[RowData]:
    game_id: int = gamedata["game_id"]
    hashes_and_moves = get_board_hashes_and_moves(gamedata, num_moves)
    data = []
    for i, hash_and_move in enumerate(hashes_and_moves, start=1):
        board_hash, (move_x, move_y) = hash_and_move
        data.append((game_id, i, board_hash, move_x, move_y))
    return data


def loader(gamedata_queue: mp.Queue, start: int, stop: int):
    batch: list[dict] = []
    for gamedata in get_gamedata(start, stop):
        batch.append(gamedata)
        if len(batch) == GAMEDATA_BATCH_SIZE:
            gamedata_queue.put(batch)
            batch = []
    if batch:
        gamedata_queue.put(batch)

    for _ in range(NUM_WORKERS):
        gamedata_queue.put(SENTINEL)

def worker(gamedata_queue: mp.Queue, results_queue: mp.Queue):
    while True:
        gamedata_batch = gamedata_queue.get()

        if gamedata_batch is SENTINEL:
            results_queue.put(SENTINEL)
            break
        
        db_data: list[RowData] = []
        for gamedata in gamedata_batch:
            db_data.extend(get_moves_data(gamedata, num_moves=NUM_MOVES))

        results_queue.put(db_data)

def writer(results_queue: mp.Queue):
    conn = psycopg2.connect(dsn)
    end_signals = 0
    insert_data: list[RowData] = []
    while end_signals < NUM_WORKERS:
        row_data = results_queue.get()
        if row_data is SENTINEL:
            end_signals += 1
            continue
        insert_data.extend(row_data)
        if len(insert_data) >= INSERT_BATCH_SIZE:
            insert_batch(conn, insert_data)
            insert_data = []
    if len(insert_data) >= INSERT_BATCH_SIZE:
            insert_batch(conn, insert_data)
    conn.close()



def run():
    start = 30_000_000
    stop = 60_000_000
    print(f"Processing {start}:{stop}")
    create_moves_table()

    # gamedata_queue: mp.Queue[list[dict]]
    # results_queue: mp.Queue[list[RowData]] 
    gamedata_queue = mp.Queue(maxsize=50) 
    results_queue = mp.Queue(maxsize=50)

    loader_proc = mp.Process(target=loader, args=(gamedata_queue, start, stop))
    writer_proc = mp.Process(target=writer, args=(results_queue,))

    loader_proc.start()
    writer_proc.start()

    workers = [mp.Process(target=worker, args=(gamedata_queue, results_queue))
               for _ in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    loader_proc.join()
    for w in workers:
        w.join()
    writer_proc.join()

if __name__ == "__main__":
    t0 = time()
    run()
    print(f"{time() - t0:.2f}")
