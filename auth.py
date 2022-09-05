import os

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from exceptions import NoProjectMembershipException

load_dotenv()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 5

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class Project(BaseModel):
    id: int
    name: str


class User(BaseModel):
    id: int
    projects: list[Project]
    is_admin: bool

    def has_project(self, project_name: str):
        return project_name in (project.name for project in self.projects)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    secret_key = os.environ["JWT_SECRET_KEY"]
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
    except JWTError as error:
        raise credentials_exception from error
    if not payload.get("user_id"):
        raise credentials_exception
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
