from fastapi import APIRouter, Depends
from auth import (
    verify_project_membership,
)
from clients.azure.data import (
    ProjectFileOrDirectory,
)
from dependencies import get_image_storage_client
from clients.azure.images import ImageStorageClient

router = APIRouter(prefix="/images", tags=["images"])


@router.get(
    "images/projects/{project_name}/object-groups/{object_group_id}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_project_object_images(
    project_name: str,
    object_group_id: int,
    azure_client: ImageStorageClient = Depends(get_image_storage_client),
):
    return azure_client.list_project_object_images(
        project_name, object_group_id, with_sas_token=True
    )


@router.get(
    "images/upload/signed-url",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_project_object_images(
    project_name: str,
    object_group_id: int,
    file_name: str,
    azure_client: ImageStorageClient = Depends(get_image_storage_client),
):
    """Returns a signed URL to upluoad an image in a project container."""
    url = azure_client.generate_signed_upload_project_object_image_url(
        project_name, object_group_id, file_name=file_name
    )
    return {"url": url}
