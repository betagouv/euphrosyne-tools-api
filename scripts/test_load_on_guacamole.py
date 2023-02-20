#!/usr/bin/env python3
"""
Create a Azure VM and a Guacamole connection
"""
import argparse
import uuid

from clients.azure import VMAzureClient
from clients.azure.vm import AzureVMDeploymentProperties
from clients.guacamole import GuacamoleClient
from .delete_vm import delete_vm
from . import get_logger
import asyncio

logger = get_logger(__name__)

PROJECTS_TO_DEPLOY = (
    "SRV_VALMONT",
    "pouet pouet",
    "test",
    "formation PIXE Dominique",
    "formation PIXE Abdelkader",
    "formation PIXE Emmie",
    "formation PIXE Christel",
    "formation PIXE Alexis",
    "formation PIXE Yoko",
    "formation PIXE Sahel",
    "formation PIXE Ruven",
    "formation PIXE Kilian",
    "formation PIXE Ina",
    "formation PIXE Yvan",
    "formation PIXE Sarah",
)

user_ids = [
    "559a068e-91e4-4597-bc90-3db543dc5a0f",
    "57b1023f-c3ae-4abc-a8e8-c176027ed5e6",
    "5b031695-4cba-4057-9328-30bae79acd89",
    "ac51bb66-6a25-4736-b60d-6191e1f27f73",
    "9e381413-740b-41df-bc85-056d615e7d62",
    "5188431e-b028-4fb5-9850-674b4f8e912b",
    "822769a8-8ac2-4c53-8782-a6187194544a",
    "3d3d6295-8e98-44b3-b539-279df5c22a32",
    "574d3207-c9d2-4843-b1fc-0ee79c455355",
    "e3df0bfb-c0ee-4b95-930c-d5753358f0c7",
    "b12f59d6-3a2b-49a7-9733-a531f32190b4",
    "79e69135-8f90-47d8-9ac2-07aba50493c6",
    "18991575-031d-4ace-8496-3081b4f7e0cd",
    "c8ff3874-97ca-45e7-be44-ecc0b40e18cc",
    "2210e5f8-b348-4cc3-a6a4-9f96f7b9a1bf",
]


def delete_vms():
    for proj in PROJECTS_TO_DEPLOY:
        delete_vm(proj)


def get_connections_urls():
    guacclient = GuacamoleClient()
    for i in range(15):
        # guacclient.create_user_if_absent(user_ids[i])
        connid = guacclient.get_connection_by_name(PROJECTS_TO_DEPLOY[i])
        # guacclient.assign_user_to_connection(connid, user_ids[i])
        link = guacclient.generate_connection_link(connid, user_ids[i])
        logger.info("%s --> %s", PROJECTS_TO_DEPLOY[i], link)


async def test_load():
    deployments: list[AzureVMDeploymentProperties] = []
    connections = {}
    for index, project in enumerate(PROJECTS_TO_DEPLOY):
        deployments.append(create_vm(project, index))
    while len(deployments):
        for index, deployment in enumerate(deployments):
            if deployment.deployment_process.done():
                logger.info("Project %s done", deployment.project_name)
                done_deployment = deployments.pop(index)
                connections[deployment.project_name] = on_deployment_done(
                    done_deployment
                )
                logger.info("%s to go", len(deployments))
        logger.info("Sleeping...")
        await asyncio.sleep(2)
    for pname, conn in connections.items():
        logger.info("%s --> %s", pname, conn)


def on_deployment_done(deployment: AzureVMDeploymentProperties):
    deployment_information = deployment.deployment_process.result()
    if deployment_information.properties.provisioning_state == "Succeeded":
        logger.info(
            "%s : VM deployed. Creating Guacamole connection...",
            deployment.project_name,
        )
        user_id = uuid.uuid4()
        guacamole_client = GuacamoleClient()
        guacamole_client.create_connection(
            name=deployment.project_name,
            ip_address=deployment_information.properties.outputs["privateIPVM"][
                "value"
            ],
            password=deployment.password,
            username=deployment.username,
        )
        connection_id = guacamole_client.get_connection_by_name(deployment.project_name)
        guacamole_client.create_user_if_absent(str(user_id))
        guacamole_client.assign_user_to_connection(connection_id, str(user_id))
        connection_link = guacamole_client.generate_connection_link(
            connection_id, str(user_id)
        )
        VMAzureClient().delete_deployment(deployment_information.name)
        return connection_link
    else:
        logger.error("Deployment failed !")


def create_vm(project_name: str, user_id: int):
    azure_client = VMAzureClient()
    logger.info("Deploying VM for project %s", project_name)
    deployment = azure_client.deploy_vm(project_name=project_name)
    if not deployment:
        logger.info("VM for project %s is already deployed.", project_name)
        return
    return deployment


if __name__ == "__main__":
    # asyncio.run(test_load())
    # get_connections_urls()
    delete_vms()
