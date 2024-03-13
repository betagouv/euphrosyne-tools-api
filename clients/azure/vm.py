import os
import re
import datetime
import logging
import concurrent.futures
from dataclasses import dataclass
from typing import Any, Literal, Optional, Callable

from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import LROPoller
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentExtended
from azure.mgmt.resource.templatespecs import TemplateSpecsClient
from dotenv import load_dotenv
from slugify import slugify

from clients import VMSizes
from clients.version import Version

load_dotenv()

logger = logging.getLogger(__name__)


PROJECT_TYPE_VM_SIZE: dict[VMSizes | None, str] = {
    None: "Standard_B8ms",  # default
    VMSizes.IMAGERY: "Standard_B20ms",
}

DeploymentStatus = Literal[
    "NotSpecified",
    "Accepted",
    "Running",
    "Ready",
    "Creating",
    "Created",
    "Canceled",
    "Failed",
    "Succeeded",
    "Updating",
]


class DeploymentNotFound(Exception):
    pass


class VMNotFound(Exception):
    pass


@dataclass
class AzureVMDeploymentProperties:
    project_name: str
    username: str
    password: str
    deployment_process: LROPoller[DeploymentExtended]
    vm_size: Optional[VMSizes] = None


@dataclass
class AzureCaptureDeploymentProperties:
    project_name: str
    version: str
    deployment_process: LROPoller[DeploymentExtended]


class VMAzureClient:
    def __init__(self):
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        credentials = DefaultAzureCredential()

        self.template_specs_name = os.environ["AZURE_TEMPLATE_SPECS_NAME"]
        self.template_specs_image_gallery = os.environ["AZURE_IMAGE_GALLERY"]
        self.template_specs_image_definition = os.environ["AZURE_IMAGE_DEFINITION"]
        self.resource_prefix = os.environ["AZURE_RESOURCE_PREFIX"]

        self._resource_mgmt_client = ResourceManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._compute_mgmt_client = ComputeManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._template_specs_client = TemplateSpecsClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )

    def list_vms(self, exclude_regex_patterns: list[str] | None = None) -> list[str]:
        if not exclude_regex_patterns:
            exclude_regex_patterns = []
        vms = self._compute_mgmt_client.virtual_machines.list(self.resource_group_name)
        filtered_vms = filter(
            lambda vm: not any(  # type: ignore
                re.match(regexp, vm.name) for regexp in exclude_regex_patterns
            ),
            vms,
        )
        return [vm.name for vm in filtered_vms]  # type: ignore

    def get_vm(self, project_name: str):
        """Retrieves VM information with project name."""
        try:
            return self._compute_mgmt_client.virtual_machines.get(
                resource_group_name=self.resource_group_name,
                vm_name=_project_name_to_vm_name(project_name),
            )
        except ResourceNotFoundError as error:
            raise VMNotFound from error

    def delete_deployment(self, project_name: str):
        return self._resource_mgmt_client.deployments.begin_delete(
            resource_group_name=self.resource_group_name,
            deployment_name=slugify(project_name),
        )

    def get_deployment_status(self, project_name: str) -> DeploymentStatus:
        """Retrieves VM information."""
        deployment = self._get_latest_ongoing_deployment_for_project(
            project_name=project_name
        )
        if not deployment:
            raise DeploymentNotFound()
        return deployment.properties.provisioning_state  # type: ignore

    def _get_latest_ongoing_deployment_for_project(
        self, project_name: str
    ) -> DeploymentExtended | None:
        """Retrieves the latest ongoing deployment for a project.
        If no deployment is found, returns None."""
        deployments = self._get_ongoing_deployments()
        project_deployments = [
            deployment
            for deployment in deployments
            if _get_project_name_from_deployment(deployment.name) == project_name  # type: ignore
        ]

        def sort_func(deployment: DeploymentExtended) -> datetime.datetime:
            if deployment.properties and deployment.properties.timestamp:
                ts: datetime.datetime = deployment.properties.timestamp
                return ts
            return datetime.datetime.min

        sorted_deployments = sorted(project_deployments, key=sort_func, reverse=True)

        if not sorted_deployments:
            return None
        return sorted_deployments[0]

    def _get_ongoing_deployments(self) -> list[DeploymentExtended]:
        statuses = [
            "Accepted",
            "Creating",
            "Created",
            "Deleting",
            "Running",
            "Ready",
            "Updating",
        ]

        deployments = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_status = {
                executor.submit(
                    self._resource_mgmt_client.deployments.list_by_resource_group,
                    self.resource_group_name,
                    f"provisioningState eq '{status}'",
                ): status
                for status in statuses
            }
            for future in concurrent.futures.as_completed(future_to_status):
                deployments += future.result()
        return deployments

    def deploy_vm(
        self,
        project_name: str,
        vm_size: Optional[VMSizes] = None,
        spec_version: Optional[str] = None,
        image_definition: str | None = None,
    ) -> Optional[AzureVMDeploymentProperties]:
        """Deploys a VM based on Template Specs specified
        with AZURE_TEMPLATE_SPECS_NAME env variable.
        In both cases where the deployment is created or it has
        already been created before, the function returns None.
        """
        try:
            status = self.get_deployment_status(project_name)
            if status in ["Running", "Ready", "Accepted", "Creating", "Updating"]:
                return None
        except DeploymentNotFound:
            # No deployment found, ok
            pass

        if (
            image_definition
            and image_definition not in self.list_vm_image_definitions()
        ):
            raise ValueError(f"Image definition {image_definition} not found")
        template = self._get_template_specs(
            template_name=self.template_specs_name, version=spec_version
        )
        parameters = {
            "vmName": slugify(project_name),
            "fileShareProjectFolder": slugify(project_name),
            "imageGallery": self.template_specs_image_gallery,
            "imageDefinition": image_definition or self.template_specs_image_definition,
            "resourcePrefix": self.resource_prefix,
            "storageAccountName": os.environ["AZURE_STORAGE_ACCOUNT"],
            "fileShareName": os.environ["AZURE_STORAGE_FILESHARE"],
            "accountName": os.environ["VM_LOGIN"],
            "accountPassword": os.environ["VM_PASSWORD"],
        }
        parameters["vmSize"] = PROJECT_TYPE_VM_SIZE[vm_size]
        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}
        poller = self._resource_mgmt_client.deployments.begin_create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=_project_name_to_deployment_name(project_name),
            parameters={
                "properties": {
                    "template": template,
                    "parameters": formatted_parameters,
                    "mode": "Incremental",
                },
            },
        )
        return AzureVMDeploymentProperties(
            project_name=project_name,
            username=os.environ["VM_LOGIN"],
            password=os.environ["VM_PASSWORD"],
            vm_size=vm_size,
            deployment_process=poller,
        )

    def delete_vm(self, project_name: str) -> Literal["Failed", "Succeeded"]:
        try:
            operation = self._compute_mgmt_client.virtual_machines.begin_delete(
                resource_group_name=self.resource_group_name,
                vm_name=_project_name_to_vm_name(project_name),
            )
        except ResourceNotFoundError as error:
            raise VMNotFound from error
        operation.result()
        return operation.status()

    def create_new_image_version(
        self,
        project_name: str,
        version: str | None = None,
        image_definition: str | None = None,
    ):
        """
        Will use the given vm to create a new specialized image of this image and save it
        to the image gallery with the given version. If no version is given, the latest
        version will be used and incremented by 1. If no image_definition is given, the
        default one will be used.
        """  # noqa: E501
        vm_name = _project_name_to_vm_name(project_name)
        template = self._get_template_specs(template_name="captureVMSpec")

        if image_definition:
            try:
                self._compute_mgmt_client.gallery_images.get(
                    resource_group_name=self.resource_group_name,
                    gallery_name=self.template_specs_image_gallery,
                    gallery_image_name=image_definition,
                )
            except ResourceNotFoundError:
                logger.info("Image %s not found, creating it...", image_definition)
                default_image = self._compute_mgmt_client.gallery_images.get(
                    resource_group_name=self.resource_group_name,
                    gallery_name=self.template_specs_image_gallery,
                    gallery_image_name=self.template_specs_image_definition,
                )
                poller = self._compute_mgmt_client.gallery_images.begin_create_or_update(
                    resource_group_name=self.resource_group_name,
                    gallery_name=self.template_specs_image_gallery,
                    gallery_image_name=image_definition,
                    gallery_image={
                        "location": default_image.location,
                        "os_state": default_image.os_state,
                        "os_type": default_image.os_type,
                        "hyper_v_generation": default_image.hyper_v_generation,
                        "identifier": {
                            "publisher": default_image.identifier.publisher,
                            "offer": default_image.identifier.offer,
                            "sku": f"euphro-{self.template_specs_image_gallery}-{image_definition}",
                        },
                    },
                )
                poller.result()
                if poller.status() != "Succeeded":
                    # pylint: disable=raise-missing-from,broad-exception-raised
                    raise Exception(f"Failed to create image {image_definition}")
                logger.info("Image %s created", image_definition)

        if version is None:
            version = self.get_latest_image_version(
                image_definition=image_definition
                or self.template_specs_image_definition
            )
            version = self.get_next_image_version(version)

        parameters = {
            "vmName": vm_name,
            "version": version,
            "galleryName": self.template_specs_image_gallery,
            "imageDefinitionName": image_definition
            or self.template_specs_image_definition,
        }

        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}

        poller = self._resource_mgmt_client.deployments.begin_create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=f"updatevmimage_{vm_name}_{version}",
            parameters={
                "properties": {
                    "template": template,
                    "parameters": formatted_parameters,
                    "mode": "Incremental",
                },
            },
        )

        return AzureCaptureDeploymentProperties(
            project_name=vm_name,
            version=version,
            deployment_process=poller,
        )

    def _get_template_specs(
        self, template_name: str, version: str | None = None
    ) -> dict[str, Any]:
        """
        Get template specs in a python dict format.
        If no version is passed, the latest one will be used.

        Parameters:
        -----------
        template_name: str
            Name of the template on Azure
        version: str
            Version of the Azure Template Specs

        Returns:
        --------
        dict[str, Any]
            Template Specs
        """

        if version is None:
            template_spec = self._template_specs_client.template_specs.get(
                resource_group_name=self.resource_group_name,
                template_spec_name=template_name,
                expand="versions",
            )

            version = sorted(
                template_spec.versions.keys(),
                key=lambda s: [int(u) for u in s.split(".")],
            )[-1]

        return self._template_specs_client.template_spec_versions.get(
            resource_group_name=self.resource_group_name,
            template_spec_name=template_name,
            template_spec_version=version,
        ).main_template

    def list_vm_image_definitions(self) -> list[str]:
        """List VM image definitions, except default one."""
        images = self._compute_mgmt_client.gallery_images.list_by_gallery(
            resource_group_name=self.resource_group_name,
            gallery_name=self.template_specs_image_gallery,
        )
        return [
            image.name
            for image in images
            if image.name != self.template_specs_image_definition
            # all images except the default one
        ]

    def _get_image_versions(
        self, gallery_name: str, gallery_image_name: str
    ) -> list[str]:
        """
        Fetch the versions available of a given image in a given gallery

        Parameters:
        -----------
        gallery_name: str
            Name of the gallery
        gallery_image_name: str
            Name of the image

        Returns:
        --------
        list[str]
            List of versions available
        """
        image_versions = (
            self._compute_mgmt_client.gallery_image_versions.list_by_gallery_image(
                resource_group_name=self.resource_group_name,
                gallery_name=gallery_name,
                gallery_image_name=gallery_image_name,
            )
        )
        return list(map(lambda img_version: str(img_version.name), image_versions))

    def get_latest_image_version(self, image_definition: str) -> str:
        """
        For the configured image gallery and image definition,
        get the latest version available

        Parameters:
        -----------
        image_definition: str
            Name of the image definition

        Returns:
        str
            Latest version available, default to 1.0.0 if none is available
        """
        versions = self._get_image_versions(
            gallery_name=self.template_specs_image_gallery,
            gallery_image_name=image_definition,
        )
        if len(versions) <= 0:
            return "1.0.0"

        parsed_versions: list[Version] = []
        for version in versions:
            try:
                parsed_versions.append(Version(version))
            except ValueError:
                pass

        parsed_versions = sorted(parsed_versions)
        latest_version = parsed_versions[-1]

        return str(latest_version)

    def get_next_image_version(self, version: str) -> str:
        """
        For the configured image gallery and image definition,
        get the next version available after the given version
        Parameters:
        -----------
        version: str
            Version to start from
        Returns:
        --------
        str
            Next version available
        """
        # Parse the version and raise error if not valid
        Version(version)
        version_components = version.split(".")
        version_components[-1] = str(int(version_components[-1]) + 1)

        return ".".join(version_components)


def wait_for_deployment_completeness(
    poller: LROPoller[DeploymentExtended],
) -> Optional[DeploymentExtended]:
    deployment = poller.result()
    if (
        deployment.properties
        and deployment.properties.provisioning_state
        and deployment.properties.provisioning_state
        in (
            "Succeeded",
            "Running",
            "Ready",
        )
    ):
        return deployment
    return None


def _project_name_to_vm_name(project_name: str):
    """Returns a correct vm name (prefix added, slugified) based on a project name"""
    # pylint: disable=consider-using-f-string
    return "{}-vm-{}".format(os.getenv("AZURE_RESOURCE_PREFIX"), slugify(project_name))


def _project_name_to_deployment_name(project_name: str):
    return f"{slugify(project_name)}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"


def _get_project_name_from_deployment(deployment_name: str):
    return "-".join(deployment_name.split("-")[:-1])
