from datetime import datetime

from azure.storage.fileshare import FileProperties


def file_properties_factory(**kwargs) -> FileProperties:
    file_properties = FileProperties()
    file_properties.name = "wallpaper.jpg"
    file_properties.path = "projects/renne-provenance/documents/wallpaper.jpg"
    file_properties.share = "euphro-stg-guacd-filestransfer"
    file_properties.snapshot = None
    file_properties.content_length = 342191
    file_properties.metadata = {}
    file_properties.file_type = "File"
    file_properties.last_modified = datetime(2023, 7, 27, 13, 18, 37)
    file_properties.etag = "abc"
    file_properties.size = 342191
    file_properties.content_range = None
    file_properties.server_encrypted = True
    file_properties.change_time = datetime(2023, 7, 27, 13, 18, 37, 570986)
    file_properties.creation_time = datetime(2023, 7, 27, 13, 18, 34, 563745)
    file_properties.last_write_time = datetime(2023, 7, 27, 13, 18, 37, 570986)
    file_properties.last_access_time = None
    file_properties.file_attributes = "Archive"
    file_properties.permission_key = "1234"
    file_properties.file_id = "1234"
    file_properties.parent_id = "1234"
    file_properties.is_directory = False
    for key, value in kwargs.items():
        setattr(file_properties, key, value)
    return file_properties
