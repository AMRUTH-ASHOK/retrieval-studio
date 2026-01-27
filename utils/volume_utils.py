"""
Unity Catalog Volume utilities for file operations

This module provides functions to upload, download, and manage files 
in Unity Catalog Volumes. It uses the Databricks SDK files API which
provides a scalable approach for handling large files without passing
them as job parameters.

Volume Path Format: /Volumes/<catalog>/<schema>/<volume>/<path>

Reference: Databricks Unity Catalog Volumes documentation
- https://docs.databricks.com/en/connect/unity-catalog/volumes.html
- https://docs.databricks.com/en/files/
"""
import os
import uuid
from typing import List, Optional, BinaryIO, Union
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import DirectoryEntry
import io


@dataclass
class UploadedFile:
    """Represents an uploaded file in a UC Volume"""
    filename: str
    volume_path: str
    size_bytes: int
    content_type: Optional[str] = None


def ensure_volume_exists(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    volume_name: str
) -> bool:
    """
    Ensure a Unity Catalog Volume exists, creating it if necessary.
    
    Args:
        w: WorkspaceClient instance
        catalog: Catalog name
        schema: Schema name  
        volume_name: Volume name
        
    Returns:
        True if volume exists or was created successfully
    """
    try:
        # Try to get the volume
        full_name = f"{catalog}.{schema}.{volume_name}"
        w.volumes.read(full_name)
        return True
    except Exception as e:
        if "VOLUME_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e).lower():
            try:
                # Create the volume (MANAGED volume type - stored in Unity Catalog)
                w.volumes.create(
                    catalog_name=catalog,
                    schema_name=schema,
                    name=volume_name,
                    volume_type="MANAGED"
                )
                print(f"[INFO] Created UC Volume: {full_name}")
                return True
            except Exception as create_error:
                print(f"[ERROR] Failed to create volume {full_name}: {create_error}")
                raise
        else:
            raise


def upload_file_to_volume(
    w: WorkspaceClient,
    file_content: Union[bytes, BinaryIO],
    volume_path: str,
    filename: str,
    overwrite: bool = True
) -> UploadedFile:
    """
    Upload a file to a Unity Catalog Volume.
    
    Uses the Databricks SDK files API to upload files directly to UC Volumes.
    This is the recommended approach for handling large files instead of
    passing them as job parameters.
    
    Args:
        w: WorkspaceClient instance
        file_content: File content as bytes or file-like object
        volume_path: Path to the volume directory (e.g., /Volumes/catalog/schema/volume/uploads)
        filename: Name of the file to create
        overwrite: Whether to overwrite existing files (default True)
        
    Returns:
        UploadedFile with details about the uploaded file
        
    Example:
        >>> uploaded = upload_file_to_volume(
        ...     w=workspace_client,
        ...     file_content=b"Hello, World!",
        ...     volume_path="/Volumes/my_catalog/my_schema/my_volume/data",
        ...     filename="example.txt"
        ... )
        >>> print(uploaded.volume_path)
        /Volumes/my_catalog/my_schema/my_volume/data/example.txt
    """
    # Ensure volume_path starts with /Volumes
    if not volume_path.startswith("/Volumes"):
        raise ValueError(f"volume_path must start with /Volumes, got: {volume_path}")
    
    # Build full file path
    full_path = f"{volume_path.rstrip('/')}/{filename}"
    
    # Convert to bytes if needed
    if hasattr(file_content, 'read'):
        content_bytes = file_content.read()
        if isinstance(content_bytes, str):
            content_bytes = content_bytes.encode('utf-8')
    elif isinstance(file_content, str):
        content_bytes = file_content.encode('utf-8')
    else:
        content_bytes = file_content
    
    # Upload using the files API
    # The files.upload method accepts a BinaryIO object
    w.files.upload(
        file_path=full_path,
        contents=io.BytesIO(content_bytes),
        overwrite=overwrite
    )
    
    return UploadedFile(
        filename=filename,
        volume_path=full_path,
        size_bytes=len(content_bytes)
    )


def upload_files_to_volume(
    w: WorkspaceClient,
    files: List[tuple],  # List of (filename, content) tuples
    volume_path: str,
    overwrite: bool = True
) -> List[UploadedFile]:
    """
    Upload multiple files to a Unity Catalog Volume.
    
    Args:
        w: WorkspaceClient instance
        files: List of (filename, content) tuples where content is bytes or file-like
        volume_path: Path to the volume directory
        overwrite: Whether to overwrite existing files
        
    Returns:
        List of UploadedFile objects
    """
    uploaded = []
    for filename, content in files:
        result = upload_file_to_volume(
            w=w,
            file_content=content,
            volume_path=volume_path,
            filename=filename,
            overwrite=overwrite
        )
        uploaded.append(result)
    return uploaded


def list_files_in_volume(
    w: WorkspaceClient,
    volume_path: str
) -> List[DirectoryEntry]:
    """
    List files in a Unity Catalog Volume directory.
    
    Args:
        w: WorkspaceClient instance
        volume_path: Path to the volume directory
        
    Returns:
        List of DirectoryEntry objects
    """
    try:
        entries = list(w.files.list_directory_contents(volume_path))
        return entries
    except Exception as e:
        if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return []
        raise


def delete_file_from_volume(
    w: WorkspaceClient,
    file_path: str
) -> bool:
    """
    Delete a file from a Unity Catalog Volume.
    
    Args:
        w: WorkspaceClient instance
        file_path: Full path to the file in the volume
        
    Returns:
        True if deleted successfully
    """
    w.files.delete(file_path)
    return True


def delete_directory_from_volume(
    w: WorkspaceClient,
    directory_path: str
) -> bool:
    """
    Delete a directory and all its contents from a Unity Catalog Volume.
    
    Args:
        w: WorkspaceClient instance
        directory_path: Full path to the directory in the volume
        
    Returns:
        True if deleted successfully
    """
    try:
        # List all files in directory
        entries = list_files_in_volume(w, directory_path)
        
        # Delete each file
        for entry in entries:
            if entry.is_directory:
                delete_directory_from_volume(w, entry.path)
            else:
                delete_file_from_volume(w, entry.path)
        
        # Delete the directory itself (if empty)
        # Note: The files API may automatically remove empty directories
        return True
    except Exception as e:
        print(f"[WARNING] Failed to delete directory {directory_path}: {e}")
        return False


def download_file_from_volume(
    w: WorkspaceClient,
    file_path: str
) -> bytes:
    """
    Download a file from a Unity Catalog Volume.
    
    Args:
        w: WorkspaceClient instance
        file_path: Full path to the file in the volume
        
    Returns:
        File content as bytes
    """
    response = w.files.download(file_path)
    return response.contents.read()


def get_volume_path_for_build(
    catalog: str,
    schema: str,
    volume_name: str,
    project_name: str,
    run_id: str
) -> str:
    """
    Get the volume path for a specific build run's uploaded files.
    
    This creates a structured path for organizing uploaded files by project and run.
    
    Args:
        catalog: Unity Catalog name
        schema: Schema name
        volume_name: Volume name
        project_name: Project name (will be sanitized)
        run_id: Build run ID
        
    Returns:
        Volume path string like /Volumes/catalog/schema/volume/uploads/project/run_id
    """
    import re
    
    # Sanitize project name
    safe_project = re.sub(r"[^a-zA-Z0-9_\-]", "_", (project_name or "").strip().lower()) or "default"
    
    return f"/Volumes/{catalog}/{schema}/{volume_name}/uploads/{safe_project}/{run_id}"


def create_manifest_file(
    w: WorkspaceClient,
    volume_path: str,
    files: List[UploadedFile],
    metadata: dict = None
) -> str:
    """
    Create a manifest file listing all uploaded files for a build run.
    
    This manifest can be used by the build notebook to know which files to process.
    
    Args:
        w: WorkspaceClient instance
        volume_path: Path to the volume directory
        files: List of uploaded files
        metadata: Additional metadata to include
        
    Returns:
        Path to the manifest file
    """
    import json
    from datetime import datetime
    
    manifest = {
        "created_at": datetime.utcnow().isoformat(),
        "files": [
            {
                "filename": f.filename,
                "path": f.volume_path,
                "size_bytes": f.size_bytes,
                "content_type": f.content_type
            }
            for f in files
        ],
        "metadata": metadata or {}
    }
    
    manifest_content = json.dumps(manifest, indent=2)
    manifest_path = f"{volume_path.rstrip('/')}/manifest.json"
    
    upload_file_to_volume(
        w=w,
        file_content=manifest_content.encode('utf-8'),
        volume_path=volume_path,
        filename="manifest.json",
        overwrite=True
    )
    
    return manifest_path
