"""
File Upload API endpoints for Unity Catalog Volumes

This module provides endpoints for uploading files to Unity Catalog Volumes.
Instead of passing large files as job parameters, files are uploaded to a 
Volume and the build job reads from the Volume path.

This approach is more scalable and follows Databricks best practices for
handling large or multiple data files.
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import List, Optional
import uuid
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from backend.auth import get_workspace_client
from backend.config import settings
from retrieval_core.configs import config as core_config
from utils.volume_utils import (
    ensure_volume_exists,
    upload_file_to_volume,
    upload_files_to_volume,
    create_manifest_file,
    get_volume_path_for_build,
    list_files_in_volume,
    delete_directory_from_volume,
    UploadedFile
)

router = APIRouter()


@router.post("/files")
async def upload_files(
    files: List[UploadFile] = File(...),
    project_name: str = Form(...),
    upload_id: Optional[str] = Form(None),
    w=Depends(get_workspace_client)
):
    """
    Upload one or more files to a Unity Catalog Volume.
    
    Files are uploaded to: /Volumes/<catalog>/<schema>/<volume>/uploads/<project>/<upload_id>/
    
    The returned upload_id and volume_path can be used when triggering a build job.
    The build job will read files from the volume instead of receiving them as parameters.
    
    Args:
        files: List of files to upload
        project_name: Project name for organizing files
        upload_id: Optional upload ID (generated if not provided)
        
    Returns:
        JSON with upload_id, volume_path, and list of uploaded files
    """
    try:
        # Generate or use provided upload_id
        upload_id = upload_id or str(uuid.uuid4())
        
        # Ensure the volume exists
        try:
            ensure_volume_exists(
                w=w,
                catalog=core_config.UC_CATALOG,
                schema=core_config.RAW_SCHEMA,
                volume_name=core_config.DATA_VOLUME_NAME
            )
        except Exception as e:
            print(f"[WARNING] Could not verify/create volume: {e}")
            # Continue - the volume might already exist
        
        # Get the volume path for this upload
        volume_path = get_volume_path_for_build(
            catalog=core_config.UC_CATALOG,
            schema=core_config.RAW_SCHEMA,
            volume_name=core_config.DATA_VOLUME_NAME,
            project_name=project_name,
            run_id=upload_id
        )
        
        # Upload each file
        uploaded_files: List[UploadedFile] = []
        
        for file in files:
            # Read file content
            content = await file.read()
            
            # Upload to volume
            result = upload_file_to_volume(
                w=w,
                file_content=content,
                volume_path=volume_path,
                filename=file.filename,
                overwrite=True
            )
            result.content_type = file.content_type
            uploaded_files.append(result)
            
            print(f"[INFO] Uploaded {file.filename} ({len(content)} bytes) to {result.volume_path}")
        
        # Create manifest file
        manifest_path = create_manifest_file(
            w=w,
            volume_path=volume_path,
            files=uploaded_files,
            metadata={
                "project_name": project_name,
                "upload_id": upload_id,
                "total_files": len(uploaded_files)
            }
        )
        
        return {
            "upload_id": upload_id,
            "volume_path": volume_path,
            "manifest_path": manifest_path,
            "files": [
                {
                    "filename": f.filename,
                    "volume_path": f.volume_path,
                    "size_bytes": f.size_bytes,
                    "content_type": f.content_type
                }
                for f in uploaded_files
            ],
            "total_files": len(uploaded_files),
            "total_size_bytes": sum(f.size_bytes for f in uploaded_files)
        }
        
    except Exception as e:
        print(f"[ERROR] File upload failed: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"File upload failed: {str(e)}")


@router.get("/files/{upload_id}")
async def get_uploaded_files(
    upload_id: str,
    project_name: str,
    w=Depends(get_workspace_client)
):
    """
    List files that were uploaded for a specific upload_id.
    
    Args:
        upload_id: The upload ID returned from upload_files
        project_name: Project name
        
    Returns:
        JSON with list of files in the upload directory
    """
    try:
        volume_path = get_volume_path_for_build(
            catalog=core_config.UC_CATALOG,
            schema=core_config.RAW_SCHEMA,
            volume_name=core_config.DATA_VOLUME_NAME,
            project_name=project_name,
            run_id=upload_id
        )
        
        entries = list_files_in_volume(w, volume_path)
        
        return {
            "upload_id": upload_id,
            "volume_path": volume_path,
            "files": [
                {
                    "name": entry.name,
                    "path": entry.path,
                    "is_directory": entry.is_directory,
                    "file_size": getattr(entry, 'file_size', None)
                }
                for entry in entries
            ]
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to list uploaded files: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")


@router.delete("/files/{upload_id}")
async def delete_uploaded_files(
    upload_id: str,
    project_name: str,
    w=Depends(get_workspace_client)
):
    """
    Delete all files for a specific upload_id.
    
    This can be used to clean up uploaded files after a build completes
    or if the user cancels the upload.
    
    Args:
        upload_id: The upload ID to delete
        project_name: Project name
        
    Returns:
        JSON confirmation
    """
    try:
        volume_path = get_volume_path_for_build(
            catalog=core_config.UC_CATALOG,
            schema=core_config.RAW_SCHEMA,
            volume_name=core_config.DATA_VOLUME_NAME,
            project_name=project_name,
            run_id=upload_id
        )
        
        success = delete_directory_from_volume(w, volume_path)
        
        return {
            "upload_id": upload_id,
            "deleted": success,
            "volume_path": volume_path
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to delete uploaded files: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete files: {str(e)}")


@router.get("/volume-info")
async def get_volume_info(w=Depends(get_workspace_client)):
    """
    Get information about the configured data volume.
    
    Returns:
        JSON with volume configuration details
    """
    try:
        volume_path = core_config.get_data_volume_path()
        
        # Try to check if volume exists
        volume_exists = False
        try:
            full_name = f"{core_config.UC_CATALOG}.{core_config.RAW_SCHEMA}.{core_config.DATA_VOLUME_NAME}"
            w.volumes.read(full_name)
            volume_exists = True
        except Exception:
            pass
        
        return {
            "catalog": core_config.UC_CATALOG,
            "schema": core_config.RAW_SCHEMA,
            "volume_name": core_config.DATA_VOLUME_NAME,
            "volume_path": volume_path,
            "volume_exists": volume_exists
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get volume info: {str(e)}")


@router.post("/volume/ensure")
async def ensure_volume(w=Depends(get_workspace_client)):
    """
    Ensure the data volume exists, creating it if necessary.
    
    Returns:
        JSON confirmation with volume details
    """
    try:
        success = ensure_volume_exists(
            w=w,
            catalog=core_config.UC_CATALOG,
            schema=core_config.RAW_SCHEMA,
            volume_name=core_config.DATA_VOLUME_NAME
        )
        
        return {
            "success": success,
            "catalog": core_config.UC_CATALOG,
            "schema": core_config.RAW_SCHEMA,
            "volume_name": core_config.DATA_VOLUME_NAME,
            "volume_path": core_config.get_data_volume_path()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to ensure volume: {str(e)}")
