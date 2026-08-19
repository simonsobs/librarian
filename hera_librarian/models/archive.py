from pydantic import BaseModel

from hera_librarian.models.admin import ManifestEntry


class ArchiveManifestRequest(BaseModel):
    librarian_name: str
    "The name of the librarian that generated this manifest."

    manifest_id: str
    "The ID of the manifest."

    archive_files: list[ManifestEntry]
    "The files on the archive."


class ArchiveManifestResponse(BaseModel):
    archive_id: str
    "The ID of the archive to get the manifest for."

    manifest_id: str
    "The ID of the manifest to get the manifest for."


class ArchiveCallbackRequest(BaseModel):
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


class ArchiveCallbackFailResponse(BaseModel):
    success: bool
    "Whether the callback was successful or not."

    reason: str
    "Reason for failure."
