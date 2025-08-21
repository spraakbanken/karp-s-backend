import re


def alphanumeric_key(key: str) -> list[int | str]:
    return [int(c) if c.isdigit() else c for c in re.split("([0-9]+)", key)]
