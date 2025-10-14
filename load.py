import gzip
import json
from collections.abc import Iterator
from typing import Any
from tqdm import tqdm
from typing import Callable

MIN_MOVES = 20
GAMES_FILE = "/data/ogs_games_2013_to_2025-05.json.gz"


def game_filter(gamedata: dict, size: int = 19, min_moves: int = MIN_MOVES) -> bool:
    if len(gamedata.get("moves", [])) < min_moves:
        return False
    if (gamedata.get("width") != size) or (gamedata.get("height") != size):
        return False
    # this should always be true, but double checking
    if "game_id" not in gamedata:
        return False
    # skip uploaded games
    if "original_sgf" in gamedata:
        return False
    # Make sure the ranked is explicitly marked as True or False
    if "ranked" not in gamedata:
        return False
    if "start_time" not in gamedata:
        return False
    # checking player_ids may be unnecessary once we've excluded original_sgfs,
    # but double checking
    if gamedata.get("white_player_id", 0) == 0:
        return False
    if gamedata.get("black_player_id", 0) == 0:
        return False
    if gamedata.get("winner", 0) == 0:
        return False
    # skip rengo games
    if gamedata.get("rengo"):
        return False
    # want handicap to be explicitly stated.
    if "handicap" not in gamedata:
        return False
    # Only even games
    if gamedata.get("handicap") != 0:
        return False
    if not isinstance(gamedata.get("komi"), (float, int)):
        return False
    if (gamedata["komi"] > 7.5) or (gamedata["komi"] < 5.5):
        return False
    if gamedata.get("initial_player", "black") != "black":
        return False
    if "initial_state" in gamedata:
        initial_state = gamedata["initial_state"]
        if initial_state["black"] or initial_state["white"]:
            return False
    return True


def get_gamedata(
    start: int, stop: int, filter: Callable | None = None
) -> Iterator[dict[str, Any]]:
    if start < 0:
        raise ValueError("start must be >= 0")
    if stop <= start:
        raise ValueError("stop must be > start")

    i = -1
    total_games = stop - start
    pbar = tqdm(total=total_games, desc="Loading games", position=0)

    with gzip.open(GAMES_FILE, "rt", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            i += 1
            if i < start:
                continue
            if i >= stop:
                break

            pbar.update(1)
            line = raw_line.strip()
            gamedata: dict = json.loads(line)
            if (filter is None) or game_filter(gamedata):
                yield gamedata
        pbar.close()


def get_gamedata_by_game_id(game_id: int) -> dict:
    with gzip.open(GAMES_FILE, "rt", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            gamedata: dict = json.loads(line)
            if gamedata["game_id"] == game_id:
                return gamedata


def get_sample_gamedata(size: int, min_moves: int) -> Iterator[dict[str, Any]]:
    total_games = 564737
    pbar = tqdm(total=total_games, desc="Loading games", position=0)

    with open(
        "/data/sample.json",
    ) as f:
        for raw_line in f:
            pbar.update(1)
            line = raw_line.strip()
            gamedata: dict = json.loads(line)
            if game_filter(gamedata, size, min_moves):
                yield gamedata
        pbar.close()
