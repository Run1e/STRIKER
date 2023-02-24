import re

dictionary = "ABCDEFGHJKLMNOPQRSTUVWXYZabcdefhijkmnopqrstuvwxyz23456789"

_bitmask64 = 2**64 - 1


def is_valid_sharecode(code):
    return bool(re.match(r"^(CSGO)?(-?[%s]{5}){5}$" % dictionary, code))


def _swap_endianness(number):
    result = 0

    for n in range(0, 144, 8):
        result = (result << 8) + ((number >> n) & 0xFF)

    return result


def decode(code):
    if not is_valid_sharecode(code):
        raise ValueError("Invalid share code")

    code = re.sub("CSGO\-|\-", "", code)[::-1]

    a = 0
    for c in code:
        a = a * len(dictionary) + dictionary.index(c)

    a = _swap_endianness(a)

    return {
        "matchId": a & _bitmask64,
        "outcomeId": a >> 64 & _bitmask64,
        "tokenId": a >> 128 & 0xFFFF,
    }
