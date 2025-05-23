class UserError(RuntimeError): ...


class InternalError(RuntimeError): ...


class Lol:
    def __init__(self, i: int, j: int):
        self.i = i
        self.j = j
