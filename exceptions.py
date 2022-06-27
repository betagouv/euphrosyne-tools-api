from fastapi import Request, status
from fastapi.responses import JSONResponse


class NoProjectMembershipException(Exception):
    pass


# pylint: disable=unused-argument
async def no_project_membership_exception_handler(
    request: Request,
    exc: NoProjectMembershipException,
):
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"detail": "User does not have access to this project"},
    )
