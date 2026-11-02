import functools
import os
import pathlib

from fastapi import APIRouter, Depends

from auth import User, get_current_user, verify_project_membership
from clients.azure.data import DataAzureClient
from path import RunDataTypeRef

# Disable libhdf5 file locking since h5grove is only reading files
# This needs to be done before any import of h5py, so before h5grove import
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

import h5grove.fastapi_utils as h5grove_fastapi  # noqa: E402,E501


def verify_file_path(
    file: str = Depends(h5grove_fastapi.add_base_path),
    current_user: User = Depends(get_current_user),
):
    ref = RunDataTypeRef.from_path(pathlib.Path(file))
    verify_project_membership(ref.project_slug, current_user)
    return file


router = APIRouter(
    route_class=h5grove_fastapi.H5GroveRoute,
    dependencies=[Depends(verify_file_path)],
    tags=["hdf5"],
    prefix="/hdf5",
)

# Clients request these paths with or without a trailing slash. Both are served
# directly so that neither depends on a redirect.
for name, endpoint in {
    "attr": h5grove_fastapi.get_attr,
    "data": h5grove_fastapi.get_data,
    "meta": h5grove_fastapi.get_meta,
    "stats": h5grove_fastapi.get_stats,
    "paths": h5grove_fastapi.get_paths,
}.items():
    router.add_api_route(f"/{name}", endpoint, methods=["GET"])
    router.add_api_route(
        f"/{name}/", endpoint, methods=["GET"], include_in_schema=False
    )


@functools.lru_cache
def resolve_filepath(filepath: str):
    return DataAzureClient().download_run_file(filepath)


h5grove_fastapi.settings.add_filepath_resolver(resolve_filepath)
