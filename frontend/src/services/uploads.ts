/**
 * File Upload API service for Unity Catalog Volumes
 * 
 * This service provides functions to upload files to UC Volumes before
 * triggering build jobs. This is a more scalable approach than passing
 * file content as job parameters.
 */
import api from './api'

export interface UploadedFileInfo {
  filename: string
  volume_path: string
  size_bytes: number
  content_type: string | null
}

export interface UploadResponse {
  upload_id: string
  volume_path: string
  manifest_path: string
  files: UploadedFileInfo[]
  total_files: number
  total_size_bytes: number
}

export interface VolumeInfo {
  catalog: string
  schema: string
  volume_name: string
  volume_path: string
  volume_exists: boolean
}

export interface ListFilesResponse {
  upload_id: string
  volume_path: string
  files: {
    name: string
    path: string
    is_directory: boolean
    file_size: number | null
  }[]
}

export const uploadsApi = {
  /**
   * Upload files to a Unity Catalog Volume
   * 
   * @param files Array of File objects to upload
   * @param projectName Project name for organizing files
   * @param uploadId Optional upload ID (auto-generated if not provided)
   * @returns Upload response with volume path and file details
   */
  async uploadFiles(
    files: File[],
    projectName: string,
    uploadId?: string
  ): Promise<UploadResponse> {
    const formData = new FormData()
    
    // Add files to form data
    files.forEach((file) => {
      formData.append('files', file)
    })
    
    // Add project name
    formData.append('project_name', projectName)
    
    // Add upload ID if provided
    if (uploadId) {
      formData.append('upload_id', uploadId)
    }
    
    const response = await api.post('/uploads/files', formData)
    
    return response.data
  },

  /**
   * List files for a specific upload
   */
  async listFiles(uploadId: string, projectName: string): Promise<ListFilesResponse> {
    const response = await api.get(`/uploads/files/${uploadId}`, {
      params: { project_name: projectName }
    })
    return response.data
  },

  /**
   * Delete uploaded files for a specific upload ID
   */
  async deleteFiles(uploadId: string, projectName: string): Promise<{ deleted: boolean }> {
    const response = await api.delete(`/uploads/files/${uploadId}`, {
      params: { project_name: projectName }
    })
    return response.data
  },

  /**
   * Get information about the configured data volume
   */
  async getVolumeInfo(): Promise<VolumeInfo> {
    const response = await api.get('/uploads/volume-info')
    return response.data
  },

  /**
   * Ensure the data volume exists (creates if necessary)
   */
  async ensureVolume(): Promise<VolumeInfo & { success: boolean }> {
    const response = await api.post('/uploads/volume/ensure')
    return response.data
  }
}

export default uploadsApi
