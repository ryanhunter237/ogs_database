from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from multiprocessing import Manager, Process, Queue
from queue import Empty
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
from time import time
from tqdm import tqdm


from load import get_gamedata
from gameproc import get_board_hashes_and_moves

INSERT_BATCH_SIZE = 5_000
SENTINEL = "__STOP__"

_RESULT_QUEUE: Optional[Queue] = None

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


def insertion_worker(result_queue: Queue, progress_queue: Queue):
    batch: list[tuple[int, int, bytes, int, int]] = []
    while True:
        item = result_queue.get()
        if item == SENTINEL:
            break
        batch.extend(item)
        if len(batch) >= INSERT_BATCH_SIZE:
            insert_batch(batch)
            progress_queue.put(len(batch))
            batch = []

    if batch:
        insert_batch(batch)
        progress_queue.put(len(batch))

    progress_queue.put(SENTINEL)


def drain_progress_queue(progress_queue: Queue, insert_pbar: tqdm, sentinel_received: bool) -> bool:
    while True:
        try:
            inserted = progress_queue.get_nowait()
        except Empty:
            break

        if inserted == SENTINEL:
            sentinel_received = True
            break

        insert_pbar.update(inserted)

    return sentinel_received


def worker_initializer(result_queue):
    global _RESULT_QUEUE
    _RESULT_QUEUE = result_queue


def process_game_and_queue_results(gamedata: dict, num_moves: int):
    if _RESULT_QUEUE is None:
        raise RuntimeError("Result queue has not been initialized")

    batch: list[tuple[int, int, bytes, int, int]] = []
    game_id: int = gamedata["game_id"]
    hashes_and_moves = get_board_hashes_and_moves(gamedata, num_moves)
    for i, hash_and_move in enumerate(hashes_and_moves, start=1):
        board_hash, (move_x, move_y) = hash_and_move
        batch.append((game_id, i, board_hash, move_x, move_y))
        if len(batch) >= INSERT_BATCH_SIZE:
            _RESULT_QUEUE.put(batch)
            batch = []

    if batch:
        _RESULT_QUEUE.put(batch)


def run():
    start = 600_000
    stop = 700_000
    print(f"Processing {start}:{stop}")
    create_moves_table()

    with Manager() as manager:
        result_queue = manager.Queue(maxsize=32)
        progress_queue: Queue = Queue()
        inserter = Process(target=insertion_worker, args=(result_queue, progress_queue), daemon=True)
        inserter.start()

        with tqdm(desc="Inserted rows", unit="row", position=1, leave=False) as insert_pbar:
            sentinel_received = False
            try:
                with ProcessPoolExecutor(8, initializer=worker_initializer, initargs=(result_queue,)) as executor:
                    futures = [
                        executor.submit(process_game_and_queue_results, gamedata, 50)
                        for gamedata in get_gamedata(start, stop)
                    ]

                    for future in as_completed(futures):
                        future.result()
                        sentinel_received = drain_progress_queue(progress_queue, insert_pbar, sentinel_received)
            finally:
                result_queue.put(SENTINEL)

            while not sentinel_received:
                inserted = progress_queue.get()
                if inserted == SENTINEL:
                    sentinel_received = True
                else:
                    insert_pbar.update(inserted)

        inserter.join()

if __name__ == "__main__":
    t0 = time()
    run()
    print(f"{time() - t0:.2f}")
