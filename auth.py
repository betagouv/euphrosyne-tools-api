import os
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader, APIKeyQuery, OAuth2PasswordBearer
from jwt import InvalidTokenError
from pydantic import BaseModel
from slugify import slugify

from clients.azure import VaultClient
from exceptions import NoProjectMembershipException

load_dotenv()

# Tokens issued by Euphrosyne are signed with its RSA private key: the Tools API
# only holds the public key and cannot sign them.
EUPHROSYNE_ALGORITHM = "RS256"
# Tokens the Tools API signs itself (path tokens, calls to Euphrosyne) use
# secrets of their own, never a secret of Euphrosyne.
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5
# Scope of the token Euphrosyne puts in EROS image URLs (see eros_proxy).
EROS_SCOPE = "tools:eros"
JWT_CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)
EUPHROSYNE_TOKEN_USER_ID_VALUE = "euphrosyne"  # user id value when decoding jwt token

api_key_header_auth = APIKeyHeader(name="X-API-KEY", auto_error=False)
api_key_query_auth = APIKeyQuery(name="api_key")
token_query_auth = APIKeyQuery(name="token")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


class Project(BaseModel):
    id: int
    name: str
    slug: str


class User(BaseModel):
    id: str
    projects: list[Project]
    is_admin: bool

    def has_project(self, project_slug: str):
        return slugify(project_slug) in (project.slug for project in self.projects)


async def get_current_user(
    jwt_token: str | None = Depends(oauth2_scheme),
    api_token: str | None = Depends(api_key_header_auth),
):
    """Defines two way to authenticate. Default is JWT token. API token can be used -
    for example for development or endpoint test via OpenAPI - by setting API_TOKEN env variable.
    """
    if not jwt_token:
        if os.getenv("API_TOKEN") and api_token == os.getenv("API_TOKEN"):
            return User(id="0", projects=[], is_admin=True)
        raise JWT_CREDENTIALS_EXCEPTION
    payload = _decode_euphrosyne_jwt(jwt_token)
    user_id = payload.get("sub") or payload.get("user_id")
    if not user_id:
        raise JWT_CREDENTIALS_EXCEPTION
    return User(
        id=str(user_id),
        projects=payload.get("projects"),
        is_admin=payload.get("is_admin"),
    )


def verify_project_membership(
    project_slug: str, current_user: User = Depends(get_current_user)
):
    if not current_user.is_admin and not current_user.has_project(project_slug):
        raise NoProjectMembershipException()


def verify_admin_permission(current_user: User = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Only admins are allowed")


def _is_euphrosyne_backend(jwt_token: str):
    """Check if the JWT token is for Euphrosyne backend."""
    payload = _decode_euphrosyne_jwt(jwt_token)
    return payload.get("user_id") == EUPHROSYNE_TOKEN_USER_ID_VALUE


def verify_is_euphrosyne_backend(jwt_token: str | None = Depends(oauth2_scheme)):
    """For euphrosyne - euphro tools communication, verify JWT token."""
    if not jwt_token:
        raise JWT_CREDENTIALS_EXCEPTION
    if not _is_euphrosyne_backend(jwt_token):
        raise HTTPException(status_code=403, detail="Not allowed")


def verify_is_euphrosyne_backend_or_admin(
    jwt_token: str | None = Depends(oauth2_scheme),
):
    """For euphrosyne - euphro tools communication, verify JWT token or admin user."""
    if not jwt_token:
        raise JWT_CREDENTIALS_EXCEPTION
    is_euphrosyne_backend = _is_euphrosyne_backend(jwt_token)

    if is_euphrosyne_backend:
        return

    payload = _decode_euphrosyne_jwt(jwt_token)
    if payload.get("is_admin"):
        return

    raise HTTPException(status_code=403, detail="Not allowed")


def verify_has_azure_permission(api_key: str | None = Depends(api_key_query_auth)):
    """
    For euphrosyne tools - Azure communication. Token is passed in the URL and checked
    aginst an Azure key vault.
    """
    if not api_key:
        raise HTTPException(status_code=403, detail="Not allowed")
    secret_api_token = VaultClient(
        f"{os.getenv('AZURE_RESOURCE_PREFIX')}-key-vault"
    ).get_secret_value("secret-api-key")
    if secret_api_token.value != api_key:
        raise HTTPException(status_code=403, detail="Not allowed")


def verify_path_permission(path: str, token: str | None = Depends(token_query_auth)):
    if not token:
        raise JWT_CREDENTIALS_EXCEPTION
    payload = _decode_path_jwt(token)
    if not payload.get("path"):
        raise JWT_CREDENTIALS_EXCEPTION
    if payload["path"] != path:
        raise HTTPException(status_code=403, detail="Token not allowed for this path")


def verify_eros_query_url_jwt(token: str | None = Depends(token_query_auth)):
    """For EROS images: a token Euphrosyne issued to a user, or the token it puts
    in image URLs, which grants only the EROS scope."""
    if not token:
        raise JWT_CREDENTIALS_EXCEPTION
    payload = _decode_euphrosyne_jwt(token)
    scopes = str(payload.get("scope") or "").split()
    if not (payload.get("sub") or payload.get("user_id")) and EROS_SCOPE not in scopes:
        raise HTTPException(status_code=403, detail="Not allowed")


def generate_token_for_euphrosyne_backend():
    """
    Generates a JWT token for Euphrosyne backend.

    Returns:
        str: The generated JWT token.

    """
    return _generate_jwt_token(
        payload={
            "user_id": EUPHROSYNE_TOKEN_USER_ID_VALUE,
        },
        secret_key=os.environ["TOOLS_API_BACKEND_TOKEN_SECRET"],
    )


def generate_token_for_path(
    path: str, data_request: str | None = None, expiration: datetime | None = None
):
    """
    Generates a JWT token for a specific path.

    Args:
        path (str): The path for which the token is generated.
        data_request (str): (Optional) The data request ID for which the token is generated.
        expiration (datetime): (Optional) The expiration date of the token.

    Returns:
        str: The generated JWT token.

    """
    payload = {
        "path": path,
    }
    if data_request:
        payload["data_request"] = data_request
    return _generate_jwt_token(
        payload=payload,
        expiration=expiration,
        secret_key=os.environ["PATH_TOKEN_SECRET_KEY"],
    )


class ExtraPayloadTokenGetter:
    def __init__(self, key: str):
        self.key = key

    def __call__(self, token: str | None = Depends(token_query_auth)):
        if token:
            payload = _decode_path_jwt(token)
            return payload.get(self.key)
        return None


def _generate_jwt_token(
    payload: dict[str, Any], secret_key: str, expiration: datetime | None = None
):
    if expiration is None:
        expiration = datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "exp": expiration,
        **payload,
    }
    return jwt.encode(payload, secret_key, algorithm=ALGORITHM)


def _decode_euphrosyne_jwt(jwt_token: str) -> dict:
    """Decode an access token issued by Euphrosyne."""
    try:
        payload = jwt.decode(
            jwt_token,
            os.environ["EUPHROSYNE_JWT_PUBLIC_KEY"].replace("\\n", "\n"),
            algorithms=[EUPHROSYNE_ALGORITHM],
            options={"require": ["exp"]},
        )
    except InvalidTokenError as error:
        raise JWT_CREDENTIALS_EXCEPTION from error
    # Refresh tokens are exchanged with Euphrosyne, never sent as bearers.
    if payload.get("token_type", "access") != "access":
        raise JWT_CREDENTIALS_EXCEPTION
    return payload


def _decode_path_jwt(jwt_token: str) -> dict:
    """Decode a path token signed by the Tools API (generate_token_for_path)."""
    try:
        return jwt.decode(
            jwt_token,
            os.environ["PATH_TOKEN_SECRET_KEY"],
            algorithms=[ALGORITHM],
            options={"require": ["exp"]},
        )
    except InvalidTokenError as error:
        raise JWT_CREDENTIALS_EXCEPTION from error
