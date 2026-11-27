# pylint: disable=protected-access, no-member, redefined-outer-name

import datetime
from unittest.mock import DEFAULT, MagicMock, Mock, create_autospec, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.resource.deployments.operations import DeploymentsOperations
from pytest import MonkeyPatch

from clients import VMSizes
from clients.azure import VMAzureClient
from clients.azure.vm import (
    DEFAULT_IMAGE_VERSIONS_TO_KEEP,
    PROJECT_TYPE_VM_SIZE,
    AzureCaptureDeploymentProperties,
    AzureVMDeploymentProperties,
    DeploymentNotFound,
    ImageVersionsPurgePlan,
    VMNotFound,
    _get_project_name_from_deployment,
    _project_name_to_deployment_name,
    _project_name_to_vm_name,
    wait_for_deployment_completeness,
)
from clients.version import InvalidVersion


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_TEMPLATE_SPECS_NAME", "template_specs")
    monkeypatch.setenv("AZURE_IMAGE_GALLERY", "image_gallery")
    monkeypatch.setenv("AZURE_IMAGE_DEFINITION", "image_definition")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "fileshare")
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "test")
    monkeypatch.setenv("VM_LOGIN", "username")
    monkeypatch.setenv("VM_PASSWORD", "password")
    with patch.multiple(
        "clients.azure.vm",
        DeploymentsMgmtClient=DEFAULT,
        ComputeManagementClient=DEFAULT,
        TemplateSpecsClient=DEFAULT,
    ):
        return VMAzureClient()


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
def test_deploy_exits_when_vm_exists(client: VMAzureClient):
    with patch.object(client, "get_deployment_status") as mock_method:
        mock_method.return_value = "Running"
        client.deploy_vm("vm-test")

    client._resource_mgmt_client.deployments.begin_create_or_update.assert_not_called()


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_proper_parameters(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    result = client.deploy_vm("vm-test", vm_size=None)

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmSize"]["value"]
        == "Standard_B8ms"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["imageGallery"]["value"]
        == "image_gallery"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["imageDefinition"]["value"]
        == "image_definition"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["resourcePrefix"]["value"]
        == "test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["storageAccountName"][
            "value"
        ]
        == "storageaccount"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["fileShareName"]["value"]
        == "fileshare"
    )
    assert not hasattr(call_args["parameters"]["properties"]["parameters"], "location")
    assert isinstance(result, AzureVMDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.username == "username"
    assert isinstance(result.password, str)
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_overriden_location(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.deploy_vm("vm-test", vm_size=VMSizes.TOMOGRAPHY)

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["location"]["value"]
        == "polandcentral"
    )


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_proper_parameters_when_imagery_project(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.deploy_vm("vm-test", vm_size=VMSizes.IMAGERY)

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmSize"]["value"]
        == PROJECT_TYPE_VM_SIZE[VMSizes.IMAGERY]
    )


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch(
    "clients.azure.vm.VMAzureClient.list_vm_image_definitions", lambda _: ["animage"]
)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_proper_parameters_when_image_definition_set(
    client: VMAzureClient,
):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.deploy_vm("vm-test", image_definition="animage")

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["imageDefinition"]["value"]
        == "animage"
    )


@pytest.mark.parametrize(
    ("features", "expected_security_type"),
    [
        (None, "Standard"),
        ([], "Standard"),
        ([("SecurityType", "TrustedLaunchSupported")], "Standard"),
        ([("DiskControllerTypes", "SCSI, NVMe")], "Standard"),
        ([("SecurityType", "TrustedLaunch")], "TrustedLaunch"),
    ],
)
@patch(
    "clients.azure.vm.VMAzureClient._get_template_specs",
    Mock(return_value={"parameters": {"securityType": {}}}),
)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_image_security_type(
    client: VMAzureClient,
    features: list[tuple[str, str]] | None,
    expected_security_type: str,
):
    def build_feature(name: str, value: str):
        feature = Mock(value=value)
        feature.name = name  # "name" is a reserved Mock constructor argument
        return feature

    client._compute_mgmt_client.gallery_images.get.return_value.features = (
        features and [build_feature(*feature) for feature in features]
    )

    client.deploy_vm("vm-test")

    client._compute_mgmt_client.gallery_images.get.assert_called_once_with(
        resource_group_name="resource_group_name",
        gallery_name="image_gallery",
        gallery_image_name="image_definition",
    )
    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["securityType"]["value"]
        == expected_security_type
    )


@patch(
    "clients.azure.vm.VMAzureClient._get_template_specs",
    Mock(return_value={"parameters": {"vmName": {}}}),
)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploy_omits_security_type_when_template_does_not_declare_it(
    client: VMAzureClient,
):
    client.deploy_vm("vm-test")

    client._compute_mgmt_client.gallery_images.get.assert_not_called()
    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert "securityType" not in call_args["parameters"]["properties"]["parameters"]


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch(
    "clients.azure.vm.VMAzureClient.list_vm_image_definitions", lambda _: ["animage"]
)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploy_raises_when_wrong_image_def(
    client: VMAzureClient,
):
    client._resource_mgmt_client.deployments.check_existence.return_value = False

    with pytest.raises(ValueError):
        client.deploy_vm("vm-test", image_definition="blabla")


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_create_image(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    result = client.create_new_image_version("vm-test", version="1.1.1")

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )

    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["version"]["value"]
        == "1.1.1"
    )
    assert isinstance(result, AzureCaptureDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.version == "1.1.1"
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
@patch(
    "clients.azure.vm.VMAzureClient.get_latest_image_version",
    Mock(return_value="1.1.1"),
)
def test_create_image_without_version(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    result = client.create_new_image_version("vm-test")

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )

    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["version"]["value"]
        == "1.1.2"
    )
    assert isinstance(result, AzureCaptureDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.version == "1.1.2"
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


@pytest.mark.parametrize(
    ("security_profile", "expected_features"),
    [
        (None, []),
        (MagicMock(security_type="Standard"), []),
        (
            MagicMock(security_type="TrustedLaunch"),
            [{"name": "SecurityType", "value": "TrustedLaunch"}],
        ),
    ],
)
@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_create_image__image_definition_matches_source_vm_security_type(
    client: VMAzureClient,
    security_profile: MagicMock | None,
    expected_features: list[dict[str, str]],
):
    def effect(*args, **kwargs):
        if kwargs["gallery_image_name"] == "new-image-def":
            raise ResourceNotFoundError()
        return MagicMock()

    client._compute_mgmt_client.gallery_images.get.side_effect = effect
    client._compute_mgmt_client.gallery_images.begin_create_or_update.return_value = (
        MagicMock(status=MagicMock(return_value="Succeeded"))
    )
    client._compute_mgmt_client.virtual_machines.get.return_value = MagicMock(
        security_profile=security_profile
    )

    client.create_new_image_version(
        "vm-test", version="0.0.1", image_definition="new-image-def"
    )

    galery_image_create_call_args = (
        client._compute_mgmt_client.gallery_images.begin_create_or_update.call_args[1]
    )
    assert (
        galery_image_create_call_args["gallery_image"]["features"] == expected_features
    )


@patch("clients.azure.vm.VMAzureClient._get_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_create_image__image_definition_creation(client: VMAzureClient):
    default_image_mock = MagicMock(
        location="location",
        os_state="os_state",
        os_type="os_type",
        hyper_v_generation="hyper_v_generation",
        identifier=MagicMock(publisher="publisher", offer="offer", sku="sku"),
    )

    def effect(*args, **kwargs):
        if kwargs["gallery_image_name"] == "new-image-def":
            raise ResourceNotFoundError()
        return default_image_mock

    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client._compute_mgmt_client.gallery_images.get.side_effect = effect
    client._compute_mgmt_client.gallery_images.begin_create_or_update.return_value = (
        MagicMock(status=MagicMock(return_value="Succeeded"))
    )

    client.create_new_image_version(
        "vm-test", version="0.0.1", image_definition="new-image-def"
    )

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["imageDefinitionName"][
            "value"
        ]
        == "new-image-def"
    )

    client._compute_mgmt_client.gallery_images.begin_create_or_update.assert_called_once()
    galery_image_create_call_args = (
        client._compute_mgmt_client.gallery_images.begin_create_or_update.call_args[1]
    )
    assert galery_image_create_call_args["gallery_image_name"] == "new-image-def"
    assert galery_image_create_call_args["gallery_image"]["location"] == "location"
    assert galery_image_create_call_args["gallery_image"]["os_state"] == "os_state"
    assert galery_image_create_call_args["gallery_image"]["os_type"] == "os_type"
    assert (
        galery_image_create_call_args["gallery_image"]["hyper_v_generation"]
        == "hyper_v_generation"
    )
    assert (
        galery_image_create_call_args["gallery_image"]["identifier"]["publisher"]
        == "publisher"
    )
    assert (
        galery_image_create_call_args["gallery_image"]["identifier"]["offer"] == "offer"
    )
    assert (
        galery_image_create_call_args["gallery_image"]["identifier"]["sku"]
        == f"euphro-{client.template_specs_image_gallery}-new-image-def"
    )


def test_list_vm_image_definitions(client: VMAzureClient):
    return_values = []
    for name in ["image1", "image2", "image_definition"]:
        value = MagicMock()
        value.name = name
        return_values.append(value)
    client._compute_mgmt_client.gallery_images.list_by_gallery.return_value = (
        return_values
    )
    assert client.list_vm_image_definitions() == ["image1", "image2"]


def test_get_template_specs(client: VMAzureClient):
    client._template_specs_client.template_specs.get.return_value.versions = {
        "1.9.0": {},
        "1.10.1": {},
        "1.11.1": {},
    }

    # Get latest
    client._get_template_specs(template_name="template_specs")
    client._template_specs_client.template_spec_versions.get.assert_called_with(
        resource_group_name="resource_group_name",
        template_spec_name="template_specs",
        template_spec_version="1.11.1",
    )

    # Get 1.9.0
    client._get_template_specs(template_name="template_specs", version="1.9.0")
    client._template_specs_client.template_spec_versions.get.assert_called_with(
        resource_group_name="resource_group_name",
        template_spec_name="template_specs",
        template_spec_version="1.9.0",
    )


def test_get_vm_raises_if_absent(client: VMAzureClient):
    client._compute_mgmt_client.virtual_machines.get.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.get_vm("VM")


@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_get_vm_calls_azure_method_with_proper_args(client: VMAzureClient):
    client.get_vm("VM")
    client._compute_mgmt_client.virtual_machines.get.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="VM",
    )


def test_get_deployment_status_returns_status(client: VMAzureClient):
    deployment_get_return = MagicMock(
        properties=MagicMock(provisioning_state="Succeeded")
    )
    with patch.object(
        client, "_get_latest_ongoing_deployment_for_project"
    ) as mock_method:
        mock_method.return_value = deployment_get_return
        status = client.get_deployment_status("VM")

    assert status == "Succeeded"


def test_get_deployment_raises_if_deployment_absent(client: VMAzureClient):
    client._resource_mgmt_client.deployments.get.side_effect = ResourceNotFoundError()

    with pytest.raises(DeploymentNotFound):
        client.get_deployment_status("VM")


def test_get_latest_ongoing_deployment_for_project(client: VMAzureClient):
    ongoing_deployments = [
        MagicMock(
            properties=MagicMock(timestamp=datetime.datetime.now(datetime.UTC)),
        ),
        MagicMock(
            properties=MagicMock(
                timestamp=datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(days=1)
            ),
        ),
    ]
    for deployment_mock in ongoing_deployments:
        deployment_mock.name = _project_name_to_deployment_name("project")
    with patch.object(
        client, "_get_ongoing_deployments", return_value=ongoing_deployments
    ):
        result = client._get_latest_ongoing_deployment_for_project("project")
    assert result == ongoing_deployments[1]


def test_get_ongoing_deployments(client: VMAzureClient):
    statuses = [
        "Accepted",
        "Creating",
        "Created",
        "Deleting",
        "Running",
        "Ready",
        "Updating",
    ]
    method_mock = create_autospec(
        object.__new__(DeploymentsOperations).list_by_resource_group,
        return_value=["deployment"],
    )
    with patch.object(
        client._resource_mgmt_client.deployments,
        "list_by_resource_group",
        new=method_mock,
    ):
        deployments = client._get_ongoing_deployments()

    assert len(deployments) == len(statuses)
    assert deployments == ["deployment"] * len(statuses)
    assert method_mock.call_count == len(statuses)
    filters_args = [call.kwargs["filter"] for call in method_mock.call_args_list]
    assert all(
        f"provisioningState eq '{status}'" in filters_args for status in statuses
    )


@pytest.mark.parametrize(
    ("status", "is_ok"),
    (("Succeeded", True), ("Running", True), ("Ready", True), ("Failed", False)),
)
def test_wait_for_deployment_completeness(status, is_ok):
    poller = MagicMock(result=MagicMock())
    poller.result.return_value = MagicMock(
        properties=MagicMock(provisioning_state=status)
    )
    deployment = wait_for_deployment_completeness(poller)
    if is_ok:
        assert deployment
    else:
        assert not deployment


def test_delete_vm(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.delete_vm("test")

    client._compute_mgmt_client.virtual_machines.begin_delete.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="test-vm-test",
    )


def test_delete_vm_raises_if_vm_absent(client: VMAzureClient):
    client._compute_mgmt_client.virtual_machines.begin_delete.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.delete_vm("vm-test")


def test_project_name_to_vm_name(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "test")
    assert _project_name_to_vm_name("BLABLA") == "test-vm-blabla"


def test_get_latest_next_versions(client: VMAzureClient):
    with patch(
        "clients.azure.vm.VMAzureClient._get_image_versions",
        Mock(return_value=["0.1.3", "1.1.4", "1.0.12"]),
    ):
        latest_version = client.get_latest_image_version("image_definition")
        assert latest_version == "1.1.4"
        next_version = client.get_next_image_version(latest_version)
        assert next_version == "1.1.5"

    with patch(
        "clients.azure.vm.VMAzureClient._get_image_versions",
        Mock(return_value=["0.12.90", "1.2.4", "1.0.12"]),
    ):
        latest_version = client.get_latest_image_version("image_definition")
        assert latest_version == "1.2.4"
        next_version = client.get_next_image_version(latest_version)
        assert next_version == "1.2.5"

    with patch(
        "clients.azure.vm.VMAzureClient._get_image_versions",
        Mock(return_value=["1.5.90", "1.2.4", "1.0.12"]),
    ):
        latest_version = client.get_latest_image_version("image_definition")
        assert latest_version == "1.5.90"
        next_version = client.get_next_image_version(latest_version)
        assert next_version == "1.5.91"


def test_get_bad_next_version(client: VMAzureClient):
    with pytest.raises(InvalidVersion):
        client.get_next_image_version("bad_version")


def test_project_name_to_deployment_name():
    with patch("clients.azure.vm.datetime") as datetime_mock:
        datetime_mock.datetime.now.return_value.strftime.return_value = "now"
        assert _project_name_to_deployment_name("projectname") == "projectname-now"


def test_get_project_name_from_deployment():
    assert _get_project_name_from_deployment("projectname-now") == "projectname"


def test_list_vms(client: VMAzureClient):
    vms = []
    for name in ["vm1", "vm2"]:
        value = MagicMock()
        value.name = name
        vms.append(value)
    client._compute_mgmt_client.virtual_machines.list.return_value = vms
    assert client.list_vms() == ["vm1", "vm2"]


def test_list_vms_with_exclude(client: VMAzureClient):
    vms = []
    for name in ["euphro-vm1", "blabla-vm2"]:
        value = MagicMock()
        value.name = name
        vms.append(value)
    client._compute_mgmt_client.virtual_machines.list.return_value = vms
    assert client.list_vms(exclude_regex_patterns=[r"euphro-.+"]) == ["blabla-vm2"]


def test_list_vms_with_created_before(client: VMAzureClient):
    now = datetime.datetime.now(datetime.UTC)
    vms = []
    for name, time_created in [
        ("eupro-prod-vm-test1", now - datetime.timedelta(hours=25)),
        ("eupro-prod-vm-test2", now - datetime.timedelta(hours=23)),
        ("eupro-prod-vm-test3", now - datetime.timedelta(hours=26)),
    ]:
        mock = MagicMock(time_created=time_created)
        mock.name = name
        vms.append(mock)

    client._compute_mgmt_client.virtual_machines.list.return_value = vms

    long_running_vms = client.list_vms(
        created_before=now - datetime.timedelta(hours=24)
    )

    assert long_running_vms == ["eupro-prod-vm-test1", "eupro-prod-vm-test3"]


def _vm_using_image(image_id: str | None, exact_version: str | None = None):
    image_reference = (
        MagicMock(id=image_id, exact_version=exact_version) if image_id else None
    )
    return MagicMock(storage_profile=MagicMock(image_reference=image_reference))


def _gallery_image_id(definition: str, version: str, gallery: str = "image_gallery"):
    return (
        "/subscriptions/ID/resourceGroups/resource_group_name/providers/"
        f"Microsoft.Compute/galleries/{gallery}/images/{definition}/versions/{version}"
    )


def test_get_image_versions_in_use(client: VMAzureClient):
    client._compute_mgmt_client.virtual_machines.list.return_value = [
        _vm_using_image(_gallery_image_id("image_definition", "1.0.2")),
        # Azure names are case insensitive
        _vm_using_image(_gallery_image_id("Other-Image", "1.0.1", "IMAGE_GALLERY")),
        # follows the latest version
        _vm_using_image(
            _gallery_image_id("image_definition", "latest"), exact_version="1.0.9"
        ),
        _vm_using_image(_gallery_image_id("image_definition", "latest")),
        # other gallery
        _vm_using_image(_gallery_image_id("image_definition", "1.0.3", "another")),
        # not created from a gallery image
        _vm_using_image("/subscriptions/ID/providers/Microsoft.Compute/images/foo"),
        _vm_using_image(None),
        MagicMock(storage_profile=None),
    ]

    assert client.get_image_versions_in_use() == {
        ("image_definition", "1.0.2"),
        ("other-image", "1.0.1"),
        ("image_definition", "1.0.9"),
    }
    client._compute_mgmt_client.virtual_machines.list.assert_called_once_with(
        "resource_group_name"
    )


def test_get_image_versions_in_use_raises_when_vms_cannot_be_listed(
    client: VMAzureClient,
):
    client._compute_mgmt_client.virtual_machines.list.side_effect = RuntimeError

    with pytest.raises(RuntimeError):
        client.get_image_versions_in_use()


def test_plan_image_versions_purge_keeps_most_recent(client: VMAzureClient):
    with patch.object(
        client,
        "_get_image_versions",
        Mock(return_value=["1.0.10", "1.0.2", "1.0.1", "1.0.9", "1.0.3"]),
    ):
        plan = client.plan_image_versions_purge(
            "image_definition", keep=3, versions_in_use=set()
        )

    # sorted as versions, not as strings
    assert plan.to_delete == ["1.0.1", "1.0.2"]
    assert plan.kept == ["1.0.3", "1.0.9", "1.0.10"]
    assert not plan.in_use
    assert not plan.ignored


def test_plan_image_versions_purge_default_keeps_three(client: VMAzureClient):
    assert DEFAULT_IMAGE_VERSIONS_TO_KEEP == 3
    versions = [f"1.0.{i}" for i in range(10)]
    with patch.object(client, "_get_image_versions", Mock(return_value=versions)):
        plan = client.plan_image_versions_purge(
            "image_definition", versions_in_use=set()
        )

    assert plan.kept == ["1.0.7", "1.0.8", "1.0.9"]
    assert plan.to_delete == versions[:7]


def test_plan_image_versions_purge_nothing_to_delete(client: VMAzureClient):
    with patch.object(
        client, "_get_image_versions", Mock(return_value=["1.0.1", "1.0.2"])
    ):
        plan = client.plan_image_versions_purge(
            "image_definition", keep=3, versions_in_use=set()
        )

    assert plan.to_delete == []
    assert plan.kept == ["1.0.1", "1.0.2"]


def test_plan_image_versions_purge_keeps_versions_used_by_vms(client: VMAzureClient):
    with patch.object(
        client,
        "_get_image_versions",
        Mock(return_value=["1.0.1", "1.0.2", "1.0.3", "1.0.4", "1.0.5"]),
    ):
        plan = client.plan_image_versions_purge(
            "Image_Definition",
            keep=2,
            versions_in_use={
                ("image_definition", "1.0.2"),
                # same version number, other image definition
                ("other", "1.0.1"),
            },
        )

    assert plan.to_delete == ["1.0.1", "1.0.3"]
    assert plan.in_use == ["1.0.2"]
    assert plan.kept == ["1.0.4", "1.0.5"]


def test_plan_image_versions_purge_fetches_versions_in_use_by_default(
    client: VMAzureClient,
):
    client._compute_mgmt_client.virtual_machines.list.return_value = [
        _vm_using_image(_gallery_image_id("image_definition", "1.0.1"))
    ]
    with patch.object(
        client,
        "_get_image_versions",
        Mock(return_value=["1.0.1", "1.0.2", "1.0.3"]),
    ):
        plan = client.plan_image_versions_purge("image_definition", keep=1)

    assert plan.to_delete == ["1.0.2"]
    assert plan.in_use == ["1.0.1"]


def test_plan_image_versions_purge_never_deletes_invalid_versions(
    client: VMAzureClient,
):
    with patch.object(
        client,
        "_get_image_versions",
        Mock(return_value=["1.0.1", "1.0.2", "1.0.3", "test-version", "2.0"]),
    ):
        plan = client.plan_image_versions_purge(
            "image_definition", keep=1, versions_in_use=set()
        )

    assert plan.to_delete == ["1.0.1", "1.0.2"]
    assert plan.ignored == ["test-version", "2.0"]
    assert plan.kept == ["1.0.3"]


@pytest.mark.parametrize("keep", [0, -1])
def test_plan_image_versions_purge_always_keeps_latest_version(
    client: VMAzureClient, keep: int
):
    with pytest.raises(ValueError):
        client.plan_image_versions_purge(
            "image_definition", keep=keep, versions_in_use=set()
        )


def test_purge_image_versions_deletes_only_planned_versions(client: VMAzureClient):
    plan = ImageVersionsPurgePlan(
        image_definition="image_definition",
        to_delete=["1.0.1", "1.0.2"],
        kept=["1.0.3"],
        in_use=["1.0.0"],
        ignored=[],
    )

    pollers = client.purge_image_versions(plan)

    delete = client._compute_mgmt_client.gallery_image_versions.begin_delete
    assert [version for version, _ in pollers] == ["1.0.1", "1.0.2"]
    assert [call.kwargs for call in delete.call_args_list] == [
        {
            "resource_group_name": "resource_group_name",
            "gallery_name": "image_gallery",
            "gallery_image_name": "image_definition",
            "gallery_image_version_name": version,
        }
        for version in ["1.0.1", "1.0.2"]
    ]


def test_list_all_vm_image_definitions_includes_default(client: VMAzureClient):
    with patch.object(client, "list_vm_image_definitions", Mock(return_value=["a"])):
        assert client.list_all_vm_image_definitions() == ["image_definition", "a"]
