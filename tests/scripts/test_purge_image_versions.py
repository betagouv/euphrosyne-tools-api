from unittest.mock import MagicMock, create_autospec

from clients.azure import VMAzureClient
from clients.azure.vm import ImageVersionsPurgePlan
from scripts.purge_image_versions import purge_image_versions


def _client(plans: dict[str, ImageVersionsPurgePlan]):
    client = create_autospec(VMAzureClient, instance=True)
    client.list_all_vm_image_definitions.return_value = list(plans)
    client.get_image_versions_in_use.return_value = {("a", "1.0.1")}
    client.plan_image_versions_purge.side_effect = lambda definition, **_: plans[
        definition
    ]
    client.purge_image_versions.side_effect = lambda plan: [
        (version, MagicMock()) for version in plan.to_delete
    ]
    return client


def _plan(definition: str, to_delete: list[str]):
    return ImageVersionsPurgePlan(
        image_definition=definition,
        to_delete=to_delete,
        kept=["1.0.9"],
        in_use=[],
        ignored=[],
    )


def test_dry_run_deletes_nothing():
    client = _client({"a": _plan("a", ["1.0.1", "1.0.2"]), "b": _plan("b", ["1.0.3"])})

    assert purge_image_versions(client, keep=2) == 3

    client.purge_image_versions.assert_not_called()
    client.plan_image_versions_purge.assert_any_call(
        "a", keep=2, versions_in_use={("a", "1.0.1")}
    )
    # VMs are only listed once for all image definitions
    client.get_image_versions_in_use.assert_called_once_with()


def test_deletes_when_not_dry_run():
    plan = _plan("a", ["1.0.1", "1.0.2"])
    client = _client({"a": plan, "b": _plan("b", [])})

    assert purge_image_versions(client, dry_run=False) == 2

    client.purge_image_versions.assert_called_once_with(plan)


def test_only_given_image_definitions():
    client = _client({"a": _plan("a", ["1.0.1"]), "b": _plan("b", ["1.0.2"])})

    purge_image_versions(client, image_definitions=["b"], dry_run=False)

    client.list_all_vm_image_definitions.assert_not_called()
    client.purge_image_versions.assert_called_once_with(_plan("b", ["1.0.2"]))
