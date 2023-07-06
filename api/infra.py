from fastapi import APIRouter, Depends

from auth import verify_has_azure_permission
from clients.azure import InfraAzureClient
from dependencies import get_infra_azure_client

router = APIRouter(prefix="/infra", tags=["infra"])


@router.post(
    "/webhooks/guacd-ip-change",
    status_code=202,
    dependencies=[Depends(verify_has_azure_permission)],
)
def update_guacamole_webapp_guacd_hostname(
    client: InfraAzureClient = Depends(get_infra_azure_client),
):
    """
    This webhook is meant to be called when Azure changes guacd configuration.
    It checks if guacamole client has correct guacd IP address in its settings
    and changes it otherwise. This prevents communication breakdown between guacd
    and guacamole client due to automatic Azure update.
    """
    guacd_ip = client.get_guacd_ip()
    client.update_guacamole_webapp_guacd_hostname(guacd_ip)
