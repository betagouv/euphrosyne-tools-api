import uuid

import pydantic
from fastapi import APIRouter, Depends, HTTPException

from auth import verify_project_membership
from clients.azure.images import ImageStorageClient
from dependencies import get_image_storage_client

router = APIRouter(prefix="/images", tags=["images"])

SUPPORTED_IMAGE_EXT = ["png", "jpg", "jpeg", "webp"]


class ListProjectObjectImagesResponse(pydantic.BaseModel):
    images: list[str]


@router.get(
    "/projects/{project_name}/object-groups/{object_group_id}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=ListProjectObjectImagesResponse,
)
async def list_project_object_images(
    project_name: str,
    object_group_id: int,
    azure_client: ImageStorageClient = Depends(get_image_storage_client),
):
    images: list[str] = []
    images_gen = azure_client.list_project_object_images(
        project_name, object_group_id, with_sas_token=True
    )
    async for image in images_gen:
        images.append(image)
    return {"images": images}


class GetUploadSignedUrlResponse(pydantic.BaseModel):
    url: str


@router.get(
    "/upload/signed-url",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=GetUploadSignedUrlResponse,
)
async def get_upload_signed_url(
    project_name: str,
    object_group_id: int,
    file_name: str,
    azure_client: ImageStorageClient = Depends(get_image_storage_client),
):
    """Returns a signed URL to upload an image in a project container."""
    file_ext = file_name.split(".")[-1]
    if not file_ext or file_ext not in SUPPORTED_IMAGE_EXT:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "File extension not supported",
                "error_code": "extension-not-supported",
            },
        )
    uiid_file_name = uuid.uuid4().hex + f".{file_ext}"
    url = await azure_client.generate_signed_upload_project_object_image_url(
        project_name, object_group_id, file_name=uiid_file_name
    )
    return {"url": url}


class GetReadonlyProjectContainerSignedUrlResponse(pydantic.BaseModel):
    base_url: str
    token: str


@router.get(
    "/projects/{project_name}/signed-url",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=GetReadonlyProjectContainerSignedUrlResponse,
)
def get_readonly_project_container_signed_url(
    project_name: str,
    azure_client: ImageStorageClient = Depends(get_image_storage_client),
):
    """Returns a signed URL to read file in a project container."""
    token = azure_client.get_project_container_sas_token(project_name)
    base_url = azure_client.get_project_container_base_url(project_name)
    return {"token": token, "base_url": base_url}
