import os
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader, APIKeyQuery, OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from slugify import slugify

from clients.azure import VaultClient
from exceptions import NoProjectMembershipException

load_dotenv()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5
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
    id: int
    projects: list[Project]
    is_admin: bool

    def has_project(self, project_name: str):
        return slugify(project_name) in (project.slug for project in self.projects)


async def get_current_user(
    jwt_token: Optional[str] = Depends(oauth2_scheme),
    api_token: Optional[str] = Depends(api_key_header_auth),
):
    """Defines two way to authenticate. Default is JWT token. API token can be used -
    for example for development or endpoint test via OpenAPI - by setting API_TOKEN env variable.
    """  # noqa: E501
    if not jwt_token:
        if os.getenv("API_TOKEN") and api_token == os.getenv("API_TOKEN"):
            return User(id=0, projects=[], is_admin=True)
        raise JWT_CREDENTIALS_EXCEPTION
    payload = _decode_jwt(jwt_token)
    if not payload.get("user_id"):
        raise JWT_CREDENTIALS_EXCEPTION
    return User(
        id=payload.get("user_id"),
        projects=payload.get("projects"),
        is_admin=payload.get("is_admin"),
    )


def verify_project_membership(
    project_name: str, current_user: User = Depends(get_current_user)
):
    if not current_user.is_admin and not current_user.has_project(project_name):
        raise NoProjectMembershipException()


def verify_admin_permission(current_user: User = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Only admins are allowed")


def verify_is_euphrosyne_backend(jwt_token: Optional[str] = Depends(oauth2_scheme)):
    """For euphrosyne - euphro tools communication, verify JWT token."""
    if not jwt_token:
        raise JWT_CREDENTIALS_EXCEPTION
    payload = _decode_jwt(jwt_token)
    if payload.get("user_id") != EUPHROSYNE_TOKEN_USER_ID_VALUE:
        raise HTTPException(status_code=403, detail="Not allowed")


def verify_has_azure_permission(api_key: Optional[str] = Depends(api_key_query_auth)):
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
    payload = _decode_jwt(token)
    if not payload.get("path"):
        raise JWT_CREDENTIALS_EXCEPTION
    if payload["path"] != path:
        raise HTTPException(status_code=403, detail="Token not allowed for this path")


def generate_token_for_path(path: str):
    """
    Generates a JWT token for a specific path.

    Args:
        path (str): The path for which the token is generated.

    Returns:
        str: The generated JWT token.

    """
    return _generate_jwt_token(
        payload={
            "path": path,
        }
    )


def _generate_jwt_token(payload: dict[str, Any]):
    return jwt.encode(payload, os.environ["JWT_SECRET_KEY"], algorithm=ALGORITHM)


def _decode_jwt(jwt_token: str):
    try:
        secret_key = os.environ["JWT_SECRET_KEY"]
        payload = jwt.decode(jwt_token, secret_key, algorithms=[ALGORITHM])
    except JWTError as error:
        raise JWT_CREDENTIALS_EXCEPTION from error
    return payload
