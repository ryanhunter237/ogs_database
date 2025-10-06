from gameproc import get_game_board, get_canonical_transform, BOARD_TRANSFORMS, MOVE_TRANSFORMS
import sente
from hashlib import blake2b

moves1 = [(3,3), (5,3), (5,5), (3,5)]
moves2 = [(3,5), (5,5), (5,3), (3,3)]
game = sente.Game(9)
for move in moves1:
    board = get_game_board(game)
    key = get_canonical_transform(board, *move)
    tfm_board = BOARD_TRANSFORMS[key](board).copy()
    tfm_move = MOVE_TRANSFORMS[key](*move)
    tfm_hash = blake2b(tfm_board.data, digest_size=8).hexdigest()
    print(tfm_hash, tfm_move)
    game.play(move[0] + 1, move[1] + 1)

game = sente.Game(9)
for move in moves2:
    board = get_game_board(game)
    key = get_canonical_transform(board, *move)
    tfm_board = BOARD_TRANSFORMS[key](board).copy()
    tfm_move = MOVE_TRANSFORMS[key](*move)
    tfm_hash = blake2b(tfm_board.data, digest_size=8).hexdigest()
    print(tfm_hash, tfm_move)
    game.play(move[0] + 1, move[1] + 1)

# moves1 = [(2,3), (15,3), (3, 16), (15,15)]
# moves2 = [(15,16), (15,3), (2,15), (3,3)]