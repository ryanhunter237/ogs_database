from functools import partial
from hashlib import blake2b

import numpy as np
import sente

# for determining canonical boards, first turn it into a 9x9 array with
# 0 for empty, 1 for black, 2 for white
# then find the board of the 8 transforms with smallest lexicographical order

NUM_MOVES = 50


def compose(f, g):
    return lambda *args, **kwargs: f(g(*args, **kwargs))


BOARD_TRANSFORMS = {
    "ID": lambda x: x,
    "ROT_90": np.rot90,
    "ROT_180": partial(np.rot90, k=2),
    "ROT_270": partial(np.rot90, k=3),
    "FLIP_T_B": np.flipud,
    "FLIP_L_R": np.fliplr,
    "FLIP_TL_BR": compose(np.rot90, np.flipud),
    "FLIP_TR_BL": compose(np.rot90, np.fliplr),
}

MOVE_TRANSFORMS = {
    "ID": lambda x, y: (x, y),
    "ROT_90": lambda x, y: (8 - y, x),
    "ROT_180": lambda x, y: (8 - x, 8 - y),
    "ROT_270": lambda x, y: (y, 8 - x),
    "FLIP_T_B": lambda x, y: (8 - x, y),
    "FLIP_L_R": lambda x, y: (x, 8 - y),
    "FLIP_TL_BR": lambda x, y: (8 - y, 8 - x),
    "FLIP_TR_BL": lambda x, y: (y, x)
}

INVERSE_MOVE_TRANSFORMS = {
    "ID": lambda x, y: (x, y),
    "ROT_90": lambda x, y: (y, 8 - x),
    "ROT_180": lambda x, y: (8 - x, 8 - y),
    "ROT_270": lambda x, y: (8 - y, x),
    "FLIP_T_B": lambda x, y: (8 - x, y),
    "FLIP_L_R": lambda x, y: (x, 8 - y),
    "FLIP_TL_BR": lambda x, y: (8 - y, 8 - x),
    "FLIP_TR_BL": lambda x, y: (y, x)
}

# relationship between BOARD_TRANSFORMS and MOVE_TRANSFORMS
# for key in BOARD_TRANSFORMS:
#     bt = BOARD_TRANSFORMS[key]
#     mt = MOVE_TRANSFORMS[key]
#     X = np.random.randint(0, 3, (9, 9), "uint8")
#     Z = X.copy()
#     move = tuple(np.random.randint(0, 9, (2,)))
#     tmove = mt(*move)
#     X[move] = 3
#     Y = bt(Z)
#     Y[tmove] = 3
#     assert np.all(bt(X) == Y)


def lexicographical_comparison(board1: np.ndarray, board2: np.ndarray) -> int:
    assert board1.shape == board2.shape
    for a, b in zip(board1.reshape(-1), board2.reshape(-1)):
        if a < b:
            return -1
        if a > b:
            return 1
    return 0


def get_canonical_transform(board: np.ndarray, move_x: int = None, move_y: int = None) -> str:
    candidates = ["ID"]
    board1 = board
    for key, transform_fn in BOARD_TRANSFORMS.items():
        # already set up candidates and board1 for "ID" transform outside loop
        if key == "ID":
            continue
        board2 = transform_fn(board)
        # keep the lexicographically smaller board
        lex_comp = lexicographical_comparison(board1, board2)
        if lex_comp > 0:  # board 2 is smaller
            candidates = [key]
            board1 = board2
        if lex_comp == 0:
            candidates.append(key)
    if len(candidates) == 1 or move_x is None or move_y is None:
        return candidates[0]
    # if multiple candidates, do a secondary search for the transform
    # which creates the lexicographically smallest move
    # this is needed for inserting symmetric positions into the database
    candidate1 = candidates[0]
    move1 = MOVE_TRANSFORMS[candidate1](move_x, move_y)
    for candidate2 in candidates[1:]:
        move2 = MOVE_TRANSFORMS[candidate2](move_x, move_y)
        if move2 < move1:
            candidate1 = candidate2
            move1 = move2
    return candidate1


def get_board_hashes_and_moves(gamedata: dict, num_moves: int = NUM_MOVES) -> list[tuple[bytes, tuple[int, int]]]:
    game = sente.Game(9)
    board_hashes_and_moves = []
    for movedata in gamedata["moves"][:num_moves]:
        try:
            x, y, _ = movedata
            # -1 <= x, y <= 8
            # -1 is pass, 0-8 are board positions
        except ValueError:
            break
        # stop if someone passes because this changes the move parity,
        # resulting in a non-standard player for a given position
        if x == -1 or y == -1:
            break
        board = get_game_board(game)
        key = get_canonical_transform(board, x, y)
        # need to copy transformed board to ensure it's a contiguous array in memory for hashing
        tfm_board = BOARD_TRANSFORMS[key](board).copy()
        tfm_move = MOVE_TRANSFORMS[key](x, y)
        tfm_board_hash = blake2b(tfm_board.data, digest_size=8).digest()
        try:
            game.play(x + 1, y + 1)
            # only add data if move is valid
            board_hashes_and_moves.append((tfm_board_hash, tfm_move))
        except sente.exceptions.IllegalMoveException as e:
            game_id = gamedata['game_id']
            # print(f"{game_id = } {movenum = }")
            # print(e)
            break

    return board_hashes_and_moves

def get_game_board(game: sente.Game) -> np.ndarray:
    board = game.numpy(["black_stones", "white_stones"])
    return board[:, :, 0] + 2 * board[:, :, 1]

# two equivalent games
# moves1 = [(3,3), (15,3), (15,15), (3,15)]
# moves2 = [(3,15), (15,15), (15,3), (3,3)]
# give the same output for this code
# game = sente.Game(19)
# for move in moves1:
#     board = get_game_board(game)
#     key = get_canonical_transform(board, *move)
#     tfm_board = BOARD_TRANSFORMS[key](board).copy()
#     tfm_move = MOVE_TRANSFORMS[key](*move)
#     tfm_hash = blake2b(tfm_board.data, digest_size=8).hexdigest()
#     print(tfm_hash, tfm_move)
#     game.play(move[0] + 1, move[1] + 1)

# another two equivalent games
# moves1 = [(2,3), (15,3), (3, 16), (15,15)]
# moves2 = [(15,16), (15,3), (2,15), (3,3)]
