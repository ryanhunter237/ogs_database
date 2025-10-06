import gzip
import json
from collections.abc import Iterator
from typing import Any

MIN_MOVES = 20
GAMES_FILE = "/data/ogs_games_2013_to_2025-05.json.gz"


def game_filter(gamedata: dict, min_moves: int = MIN_MOVES) -> bool:
    if len(gamedata.get("moves", [])) < min_moves:
        return False
    # only want 9x9 games
    if (gamedata.get("width", 9) != 9) or (gamedata.get("height", 9) != 9):
        return False
    # this should always be true, but double checking
    if "game_id" not in gamedata:
        return False
    # skip uploaded games
    if "original_sgf" in gamedata:
        return False
    # checking player_ids may be unnecessary once we've excluded original_sgfs,
    # but double checking
    if gamedata.get("white_player_id", 0) == 0:
        return False
    if gamedata.get("black_player_id", 0) == 0:
        return False
    # want handicap to be explicitly stated.
    if "handicap" not in gamedata:
        return False
    # only want even games
    if gamedata["handicap"] != 0:
        return False
    if gamedata.get("initial_player", "black") != "black":
        return False
    if "initial_state" in gamedata:
        initial_state = gamedata["initial_state"]
        if initial_state["black"] or initial_state["white"]:
            return False
    return True

def get_gamedata(num_games: int | None = None) -> Iterator[dict[str, Any]]:
    if num_games and num_games < 1:
        raise ValueError("num_games must be None or >= 1")
    i = 0
    with gzip.open(GAMES_FILE, "rt", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            gamedata: dict = json.loads(line)
            if game_filter(gamedata):
                yield gamedata
                i += 1
                if i == num_games:
                    break
