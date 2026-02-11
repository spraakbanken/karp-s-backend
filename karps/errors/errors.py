from dataclasses import dataclass
from typing import Any


class UserError(RuntimeError): ...


class CodeUserError(Exception):
    def __init__(self, msg: str, details: dict[str, Any] | None = None):
        self.code = error_codes[self.__class__].code
        self.msg = msg
        self.details = details
        super().__init__(msg)


class GroupConcatError(CodeUserError):
    def __init__(self):
        super().__init__("too many rows per cell (group_concat_max_len was exceeded)")


class UserAccessError(CodeUserError):
    def __init__(self, resource: str):
        super().__init__(f"user does not have access to resource: {resource}", details={"resource": resource})


class JWTError(CodeUserError):
    def __init__(self):
        super().__init__("JWT was malformed or expired")


class ApiKeyError(CodeUserError):
    def __init__(self):
        super().__init__("API key was malformed, expired or it was not possible to verify key.")


@dataclass
class ErrorRep:
    code: int
    description: str


error_codes: dict[type[CodeUserError], ErrorRep] = {
    GroupConcatError: ErrorRep(
        1,
        'Returned when the database was forced to truncate a value. Query parameter "columns" is the issue.',
    ),
    UserAccessError: ErrorRep(
        2,
        "Returned when an unauthenticated user or a user without the proper access tries access a restricted resource.",
    ),
    JWTError: ErrorRep(3, "Returned when a JWT was given, but the JWT was malformed or expired."),
    ApiKeyError: ErrorRep(
        4, "Returned when an API key was given, but it was malformed, expired or it was not possible to verify the key."
    ),
}


class InternalError(RuntimeError): ...
