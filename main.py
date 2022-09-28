import os

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import config, connect, data, deployments, vms
from exceptions import (
    NoProjectMembershipException,
    no_project_membership_exception_handler,
)

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=0.1,
)

app = FastAPI()

app.add_exception_handler(
    NoProjectMembershipException, no_project_membership_exception_handler
)

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
