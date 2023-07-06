import functools
import os

from fastapi import APIRouter, Depends

from auth import User, get_current_user, verify_project_membership
from clients.azure.data import (
    DataAzureClient,
    IncorrectDataFilePath,
    validate_run_data_file_path,
)
from exceptions import NoProjectMembershipException

# Disable libhdf5 file locking since h5grove is only reading files
# This needs to be done before any import of h5py, so before h5grove import
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

import h5grove.fastapi_utils as h5grove_fastapi  # noqa


def verify_file_path(
    file: str = Depends(h5grove_fastapi.add_base_path),
    current_user: User = Depends(get_current_user),
):
    try:
        validate_run_data_file_path(file, current_user)
    except IncorrectDataFilePath as error:
        raise NoProjectMembershipException from error
    return file


router = APIRouter(
    route_class=h5grove_fastapi.H5GroveRoute,
    dependencies=[Depends(verify_file_path)],
    tags=["hdf5"],
    prefix="/hdf5",
)

router.add_api_route("/attr/", h5grove_fastapi.get_attr, methods=["GET"])
router.add_api_route("/data/", h5grove_fastapi.get_data, methods=["GET"])
router.add_api_route("/meta/", h5grove_fastapi.get_meta, methods=["GET"])
router.add_api_route("/stats/", h5grove_fastapi.get_stats, methods=["GET"])
router.add_api_route("/paths/", h5grove_fastapi.get_paths, methods=["GET"])


@functools.lru_cache
def resolve_filepath(filepath: str):
    return DataAzureClient().download_run_file(filepath)


h5grove_fastapi.settings.add_filepath_resolver(resolve_filepath)
