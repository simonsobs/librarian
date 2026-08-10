from pydantic import BaseModel

from hera_librarian.models.admin import ManifestEntry


class ArchiveManifestRequest(BaseModel):
    archive_name: str
    "The name of the archive to get the manifest for."

    maximum_size: int = 0
    "The maximum size of the archive in bytes."


class ArchiveManifestResponse(BaseModel):
    librarian_name: str
    "The name of the librarian that generated this manifest."

    manifest_id: str
    "The ID of the manifest."

    archive_name: str
    "The name of the archive."

    archive_files: list[ManifestEntry]
    "The files on the archive."


class ArchiveCallbackRequest(BaseModel):
    archive_name: str
    "The name of the archive to call back."

    manifest_id: str
    "The ID of the manifest."

    archive_id: str
    "The ID of the archive."

    archive_path: str
    "The path to the archive on the store."


class ArchiveCallbackResponse(BaseModel):
    success: bool
    "Whether the callback was successful or not."

    message: str
    "A message describing the result of the callback."
