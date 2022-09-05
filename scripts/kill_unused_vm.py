#!/usr/bin/env python3
"""
Check if there is VM without a connected user for more than 30min.
If those VM are not in a group to indicate that it should be kept awake,
shut it down and destroy the connection in Guacamole
"""
import asyncio
import time
from typing import Any, Coroutine

from clients.guacamole import GuacamoleClient
from scripts.delete_vm import delete_vm

from . import get_logger

logger = get_logger(__name__)


async def kill_unused_vm():
    logger.info("Querying connections in Guacamole")

    guacamole_client = GuacamoleClient()

    data = guacamole_client.get_connections_and_groups()
    child_connection_groups = data["childConnectionGroups"]

    if len(child_connection_groups) <= 0:
        logger.info("No groups found")
        return

    project_to_kill: list[Coroutine[Any, Any, None]] = []

    for connection_group in child_connection_groups:
        if connection_group["name"] != "imagery":
            if "childConnections" not in connection_group:
                # No connections in the group
                continue
            # Check that we are not in the imagery group
            for connection in connection_group["childConnections"]:
                if connection["activeConnections"] > 0:
                    continue
                # There is nobody connected

                if "lastActive" not in connection:
                    # No lastActive, which mean nobody has connected to this vm
                    continue

                last_active = connection["lastActive"]
                now_in_millsecond = round(time.time() * 1000)
                delay = 30 * 60 * 1000  # 30min in ms

                if now_in_millsecond - last_active >= delay:
                    # VM hasn't seen activity for the last 30min, we can delete it
                    project_name = connection["name"]
                    logger.info("%s is unused, will be disconnected", project_name)
                    project_to_kill.append(
                        async_delete_vm(project_name, guacamole_client=guacamole_client)
                    )

    if len(project_to_kill) <= 0:
        logger.info("No VM to shutdown")
        return

    await asyncio.gather(*project_to_kill)
    logger.info("Done shutting down vm")


async def async_delete_vm(project_name: str, guacamole_client: GuacamoleClient):
    delete_vm(project_name, guacamole_client=guacamole_client)


if __name__ == "__main__":
    asyncio.run(kill_unused_vm())
