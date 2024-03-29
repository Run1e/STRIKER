import re

dictionary = "ABCDEFGHJKLMNOPQRSTUVWXYZabcdefhijkmnopqrstuvwxyz23456789"
sharecode_re = r"^CSGO(-[" + dictionary + r"]{5}){5}$"

_bitmask64 = 2**64 - 1


def is_valid_sharecode(code):
    return bool(re.match(sharecode_re, code))


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
        "id": a & _bitmask64,
        "outcome_id": a >> 64 & _bitmask64,
        "token": a >> 128 & 0xFFFF,
    }
