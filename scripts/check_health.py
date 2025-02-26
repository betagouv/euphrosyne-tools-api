#!/usr/bin/env python3
"""
Healthcheck script. Check Azure & Guacamole services are up
and correctly set up ; send an alert otherwise.
"""
import dataclasses
import os

import sentry_sdk
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

from clients.guacamole import GuacamoleAuthenticationError, GuacamoleClient

from . import get_logger

load_dotenv()

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=0.1,
    environment=os.getenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "dev"),
)

logger = get_logger(__name__)


@dataclasses.dataclass
class HealthCheckStatus:
    azure: bool = True
    guacamole: bool = True

    # pylint: disable=invalid-name
    @property
    def ok(self) -> bool:
        return self.azure and self.guacamole


class HealthCheckException(Exception):
    def __init__(self, status: HealthCheckStatus, *args, **kwargs):
        whos_down: list[str] = []
        if not status.azure:
            whos_down.append("azure")
        if not status.guacamole:
            whos_down.append("guacamole")
        formatted_whos_down = ", ".join(whos_down)
        message = f"Service(s) down: {formatted_whos_down}"
        super().__init__(message, *args, **kwargs)


def check_health():
    status = HealthCheckStatus()

    # Check Azure authentication
    try:
        DefaultAzureCredential().get_token("https://management.azure.com/.default")
    except ClientAuthenticationError as error:
        status.azure = False
        logger.error(error)

    # Check Guacamole authentication
    if os.get_env("GUACAMOLE_ENABLED", ""):
        try:
            # pylint: disable=protected-access
            GuacamoleClient()._get_admin_token()
        except GuacamoleAuthenticationError as error:
            status.guacamole = False
            logger.error(error)
    else:
        logger.warning("[check_health] Guacamole is disabled. Skipping.")

    if not status.ok:
        raise HealthCheckException(status)


if __name__ == "__main__":
    check_health()
