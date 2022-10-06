#!/usr/bin/env python3
"""
Check if there is VM without a connected user for more than 30min.
If those VM are not in a group to indicate that it should be kept awake,
shut it down and destroy the connection in Guacamole
"""
import asyncio
from typing import Any, Coroutine

from clients.guacamole import GuacamoleClient
from scripts.delete_vm import delete_vm

from . import get_logger

logger = get_logger(__name__)


async def kill_unused_vm():
    logger.info("Querying connections in Guacamole")

    guacamole_client = GuacamoleClient()

    projects_to_shutdown = guacamole_client.get_vm_to_shutdown()

    if len(projects_to_shutdown) <= 0:
        logger.info("No VM to shutdown")
        return

    tasks_to_shutdown: list[Coroutine[Any, Any, None]] = map(
        lambda project_name: async_delete_vm(
            project_name, guacamole_client=guacamole_client
        ),
        projects_to_shutdown,
    )

    await asyncio.gather(*tasks_to_shutdown)
    logger.info("Done shutting down vm")


async def async_delete_vm(project_name: str, guacamole_client: GuacamoleClient):
    delete_vm(project_name, guacamole_client=guacamole_client)


if __name__ == "__main__":
    asyncio.run(kill_unused_vm())
