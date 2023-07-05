from clients.azure.vm import VMAzureClient
from . import get_logger

logger = get_logger(__name__)


def get_latest_version():
    azure_client = VMAzureClient()
    azure_client.get_latest_image_version()


if __name__ == "__main__":
    get_latest_version()
