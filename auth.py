import os
from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Any

import jwt
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader, APIKeyQuery, OAuth2PasswordBearer
from jwt import PyJWKClient, PyJWTError
from pydantic import BaseModel
from slugify import slugify

from clients.azure import VaultClient
from exceptions import NoProjectMembershipException

load_dotenv()

# Tokens issued by Euphrosyne (its OpenID Connect provider) are signed with its
# RSA key. The Tools API reads the public keys at /.well-known/jwks.json.
EUPHROSYNE_ALGORITHM = "RS256"
# Path tokens, which the Tools API signs and verifies itself.
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5
# Scopes of the tokens Euphrosyne issues for the Tools API.
TOOLS_SCOPE = "tools"  # a user: web front end, Herma
BACKEND_SCOPE = "tools:backend"  # Euphrosyne itself
EROS_SCOPE = "tools:eros"  # token Euphrosyne puts in EROS image URLs
JWT_CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)
# User the Euphrosyne backend acts as.
EUPHROSYNE_BACKEND_USER = "euphrosyne"

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
    if _is_backend_payload(payload):
        return User(id=EUPHROSYNE_BACKEND_USER, projects=[], is_admin=True)
    if not _is_user_payload(payload):
        raise JWT_CREDENTIALS_EXCEPTION
    return User(
        id=payload["sub"],
        projects=payload.get("projects") or [],
        is_admin=bool(payload.get("is_admin")),
    )


def verify_project_membership(
    project_slug: str, current_user: User = Depends(get_current_user)
):
    if not current_user.is_admin and not current_user.has_project(project_slug):
        raise NoProjectMembershipException()


def verify_admin_permission(current_user: User = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Only admins are allowed")


def _scopes(payload: dict) -> set[str]:
    return set(str(payload.get("scope") or "").split())


def _is_user_payload(payload: dict) -> bool:
    """A token issued to a user for the Tools API (not to another application)."""
    return bool(payload.get("sub")) and TOOLS_SCOPE in _scopes(payload)


def _is_backend_payload(payload: dict) -> bool:
    return not payload.get("sub") and BACKEND_SCOPE in _scopes(payload)


def _is_euphrosyne_backend(jwt_token: str):
    """Check if the JWT token is for Euphrosyne backend."""
    return _is_backend_payload(_decode_euphrosyne_jwt(jwt_token))


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
    payload = _decode_euphrosyne_jwt(jwt_token)
    if _is_backend_payload(payload):
        return
    if _is_user_payload(payload) and payload.get("is_admin"):
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
    if not _is_user_payload(payload) and EROS_SCOPE not in _scopes(payload):
        raise HTTPException(status_code=403, detail="Not allowed")


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
        secret_key=os.environ["PATH_TOKEN_SECRET"],
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


def euphrosyne_url() -> str:
    return os.environ["EUPHROSYNE_BACKEND_URL"].rstrip("/")


@cache
def _jwks_client() -> PyJWKClient:
    # Keys are cached; a token signed with a new key (rotation) makes the
    # client fetch the key set again.
    return PyJWKClient(f"{euphrosyne_url()}/.well-known/jwks.json", lifespan=3600)


def _decode_euphrosyne_jwt(jwt_token: str) -> dict:
    """Decode an access token issued by Euphrosyne."""
    try:
        key = _jwks_client().get_signing_key_from_jwt(jwt_token).key
        payload = jwt.decode(
            jwt_token,
            key,
            algorithms=[EUPHROSYNE_ALGORITHM],
            # The keys come from this URL: only its tokens are accepted.
            issuer=euphrosyne_url(),
            options={"require": ["exp", "iss"]},
        )
    except PyJWTError as error:
        raise JWT_CREDENTIALS_EXCEPTION from error
    if payload.get("token_use") != "access":
        raise JWT_CREDENTIALS_EXCEPTION
    return payload


def _decode_path_jwt(jwt_token: str) -> dict:
    """Decode a path token signed by the Tools API (generate_token_for_path)."""
    try:
        return jwt.decode(
            jwt_token,
            os.environ["PATH_TOKEN_SECRET"],
            algorithms=[ALGORITHM],
            options={"require": ["exp"]},
        )
    except PyJWTError as error:
        raise JWT_CREDENTIALS_EXCEPTION from error
