from enum import Enum


class StorageRole(str, Enum):
    HOT = "HOT"
    COOL = "COOL"


class StorageBackend(str, Enum):
    AZURE_FILESHARE = "AZURE_FILESHARE"
    AZURE_BLOB = "AZURE_BLOB"
