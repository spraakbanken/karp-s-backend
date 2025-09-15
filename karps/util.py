import locale
import re

# set the locale category for sortings strings to Swedish
locale.setlocale(locale.LC_COLLATE, "sv_SE.UTF-8")


def alphanumeric_key(key: str) -> list[int | str]:
    # Split string into numbers and non-numbers. Let the numbers represent themselves and use locale.strxfrm for non-numbers
    return [int(part) if part.isdigit() else locale.strxfrm(part) for part in re.split("([0-9]+)", key)]
