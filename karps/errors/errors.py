class UserError(RuntimeError): ...


class CodeUserError(Exception):
    def __init__(self, code: int, msg: str):
        self.code = code
        self.msg = msg
        super().__init__(msg)


class GroupConcatError(CodeUserError):
    def __init__(self):
        super().__init__(1, "too many rows per cell (group_concat_max_len was exceeded)")


class InternalError(RuntimeError): ...
