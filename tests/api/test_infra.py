from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import verify_has_azure_permission
from dependencies import get_infra_azure_client


def override_verify_project_membership():
    pass


def test_update_guacamole_webapp_guacd_hostname(app: FastAPI, client: TestClient):
    app.dependency_overrides[verify_has_azure_permission] = (
        override_verify_project_membership
    )

    update_guacamole_webapp_guacd_hostname_mock = MagicMock()
    app.dependency_overrides[get_infra_azure_client] = lambda: MagicMock(
        get_guacd_ip=MagicMock(return_value="abc"),
        update_guacamole_webapp_guacd_hostname=update_guacamole_webapp_guacd_hostname_mock,
    )
    with patch("auth.verify_has_azure_permission"):
        response = client.post("/infra/webhooks/guacd-ip-change?api_token=token")

    assert response.status_code == 202
    update_guacamole_webapp_guacd_hostname_mock.assert_called_with("abc")

    del app.dependency_overrides[verify_has_azure_permission]
