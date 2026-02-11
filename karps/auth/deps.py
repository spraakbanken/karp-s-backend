from fastapi import Depends
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from karps import config
from karps.errors.errors import UserError


auth_scheme = HTTPBearer(auto_error=False)
api_key_scheme = APIKeyHeader(name="X-Api-Key", auto_error=False)


def get_allowed_resources(
    credentials: HTTPAuthorizationCredentials | None = Depends(auth_scheme),
    api_key: str | None = Depends(api_key_scheme),
) -> list[str]:
    """
    Used with Depends in FastAPI to provide both documentation
    and a list of allowed resources to check against, if the request includes
    a resource with limited access.
    """
    if credentials or api_key:
        env = config.get_env()

        if credentials:
            if not env.auth_jwt_pubkey_path:
                raise UserError("JWT auth not set up on instance")
            from karps.auth import sbauth_jwt

            scope = sbauth_jwt.get_scope(credentials)
        else:
            if not (env.sbauth_api_key and env.sbauth_url):
                raise UserError("API key auth not set up on instance")
            from karps.auth import sbauth_api_key

            scope = sbauth_api_key.get_scope(api_key, env.sbauth_url, env.sbauth_api_key)

        if "lexica" in scope:
            return scope["lexica"].keys()
        else:
            return []
    else:
        return []
