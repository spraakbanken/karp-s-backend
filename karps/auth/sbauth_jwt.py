from pathlib import Path
from typing import Any, Mapping
import jwt
import jwt.exceptions as jwte
from karps.config import get_env
from karps.errors.errors import JWTError


def load_jwt_key(path: Path | None) -> str:
    if path is None:
        raise RuntimeError(f"{keyname.upper()} not set")
    with open(path) as fp:
        return fp.read()


keyname = "auth_jwt_pubkey_path"
_jwt_key = load_jwt_key(getattr(get_env(), keyname))


def get_scope(credentials) -> Mapping[str, Any]:
    try:
        jwt_decoded = jwt.decode(credentials.credentials, key=_jwt_key, algorithms=["RS256"], leeway=5)
    except (jwte.ExpiredSignatureError, jwte.DecodeError):
        raise JWTError()
    return jwt_decoded.get("scope", {})
