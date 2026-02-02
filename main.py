import os

import sentry_sdk
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api import config, connect, data, deployments, hdf5, images, infra, vms
from exceptions import NoProjectMembershipException
from api import eros

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=0.1,
    environment=os.getenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "dev"),
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOWED_ORIGIN", "").split(" "),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(vms.router)
app.include_router(connect.router)
app.include_router(deployments.router)
app.include_router(data.router)
app.include_router(config.router)
app.include_router(infra.router)
app.include_router(hdf5.router)
app.include_router(images.router)
app.include_router(eros.router)


@app.exception_handler(NoProjectMembershipException)
# pylint: disable=unused-argument
async def no_project_membership_exception_handler(
    request: Request,
    exc: NoProjectMembershipException,
):
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"detail": "User does not have access to this project"},
    )
