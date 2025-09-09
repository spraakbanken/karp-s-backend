import locale

# set the locale category for sortings strings to Swedish
locale.setlocale(locale.LC_COLLATE, "sv_SE.UTF-8")


def alphanumeric_key(key: str) -> list[int | str]:
    return [int(c) if c.isdigit() else c for c in locale.strxfrm(key)]
