import { useState, useEffect, useRef, useCallback } from 'react'
import { PlayCircle, ChevronRight, ChevronLeft, CheckCircle2, ExternalLink, Clock, Copy, CheckCircle, Plus, Trash2, Upload, File, X, AlertCircle, Loader2 } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { metadataApi } from '../services/metadata'
import { uploadsApi, UploadResponse } from '../services/uploads'
import { DataType, Strategy } from '../types'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { useNavigate } from 'react-router-dom'

// File upload state interface
interface UploadedFileState {
  file: File
  status: 'pending' | 'uploading' | 'uploaded' | 'error'
  error?: string
}

export default function Build() {
  const navigate = useNavigate()
  const { selectedProject, selectedProjectId } = useProject()
  const [activeStep, setActiveStep] = useState(0)
  const [dataTypes, setDataTypes] = useState<DataType[]>([])
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [selectedDataType, setSelectedDataType] = useState('')
  const [selectedStrategies, setSelectedStrategies] = useState<string[]>([])
  const [embeddingEndpoint, setEmbeddingEndpoint] = useState('')
  const [vsEndpoint, setVsEndpoint] = useState('')
  const [dataSources, setDataSources] = useState<Array<{ id: string; config: Record<string, any> }>>([
    { id: crypto.randomUUID(), config: {} }
  ])
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
  
  // File upload state for UC Volume upload
  const [selectedFiles, setSelectedFiles] = useState<UploadedFileState[]>([])
  const [uploadProgress, setUploadProgress] = useState<{ current: number; total: number } | null>(null)
  const [uploadedVolumePath, setUploadedVolumePath] = useState<string | null>(null)
  const [isUploading, setIsUploading] = useState(false)
  const [isDragOver, setIsDragOver] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [submittedRunId, setSubmittedRunId] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<{
    state: string
    job_url: string | null
    start_time: number | null
    status: any
    run_id: string
  } | null>(null)
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null)

  const steps = [
    { label: 'Select Data Type', number: 1 },
    { label: 'Configure Data Source', number: 2 },
    { label: 'Select Strategies', number: 3 },
    { label: 'Configure & Run', number: 4 },
  ]

  useEffect(() => {
    loadMetadata()
    
    // Cleanup polling on unmount
    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current)
      }
    }
  }, [])

  useEffect(() => {
    // Start polling when a run is submitted
    if (submittedRunId) {
      pollJobStatus()
      pollingIntervalRef.current = setInterval(pollJobStatus, 5000) // Poll every 5 seconds
    }

    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current)
        pollingIntervalRef.current = null
      }
    }
  }, [submittedRunId])

  useEffect(() => {
    // Reset data sources and files when data type changes
    if (selectedDataType) {
      setDataSources([{ id: crypto.randomUUID(), config: {} }])
      setSelectedFiles([])
      setUploadedVolumePath(null)
    }
  }, [selectedDataType])

  // File upload handlers
  const handleFileSelect = useCallback((files: FileList | null) => {
    if (!files) return
    
    const newFiles: UploadedFileState[] = Array.from(files).map(file => ({
      file,
      status: 'pending' as const
    }))
    
    setSelectedFiles(prev => [...prev, ...newFiles])
    // Reset upload state when new files are added
    setUploadedVolumePath(null)
  }, [])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
    handleFileSelect(e.dataTransfer.files)
  }, [handleFileSelect])

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(true)
  }, [])

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setIsDragOver(false)
  }, [])

  const removeFile = useCallback((index: number) => {
    setSelectedFiles(prev => prev.filter((_, i) => i !== index))
    setUploadedVolumePath(null)
  }, [])

  const uploadFilesToVolume = async (): Promise<string | null> => {
    if (!selectedProject || selectedFiles.length === 0) return null
    
    setIsUploading(true)
    setUploadProgress({ current: 0, total: selectedFiles.length })
    
    try {
      // Get files that are pending or errored
      const filesToUpload = selectedFiles
        .filter(f => f.status === 'pending' || f.status === 'error')
        .map(f => f.file)
      
      if (filesToUpload.length === 0 && uploadedVolumePath) {
        // All files already uploaded
        return uploadedVolumePath
      }

      // Upload all files at once
      const response: UploadResponse = await uploadsApi.uploadFiles(
        filesToUpload,
        selectedProject.project_name
      )
      
      // Update file states to uploaded
      setSelectedFiles(prev => 
        prev.map(f => ({
          ...f,
          status: 'uploaded' as const
        }))
      )
      
      setUploadedVolumePath(response.volume_path)
      setUploadProgress({ current: filesToUpload.length, total: filesToUpload.length })
      
      console.log(`[INFO] Uploaded ${response.total_files} files to ${response.volume_path}`)
      return response.volume_path
      
    } catch (error) {
      console.error('Failed to upload files:', error)
      // Mark files as errored
      setSelectedFiles(prev => 
        prev.map(f => ({
          ...f,
          status: 'error' as const,
          error: 'Upload failed'
        }))
      )
      throw error
    } finally {
      setIsUploading(false)
    }
  }

  // Check if current data type requires file upload
  const isFileUploadDataType = ['csv', 'json', 'pdf'].includes(selectedDataType)

  const pollJobStatus = async () => {
    if (!submittedRunId) return
    
    try {
      const status = await buildsApi.getStatus(submittedRunId)
      setJobStatus({
        state: status.state,
        job_url: status.job_url,
        start_time: status.start_time,
        status: status.status,
        run_id: status.run_id,
      })
      
      // If job succeeded, stop polling and navigate to evaluate page
      if (status.state === 'SUCCESS') {
        if (pollingIntervalRef.current) {
          clearInterval(pollingIntervalRef.current)
          pollingIntervalRef.current = null
        }
        // Navigate to evaluate page after a short delay
        setTimeout(() => {
          navigate('/evaluate')
        }, 2000)
      } else if (status.state === 'FAILED') {
        // Stop polling on failure
        if (pollingIntervalRef.current) {
          clearInterval(pollingIntervalRef.current)
          pollingIntervalRef.current = null
        }
      }
    } catch (error) {
      console.error('Failed to poll job status:', error)
    }
  }

  const formatTimeSince = (startTime: number | null) => {
    if (!startTime) return 'N/A'
    const seconds = Math.floor((Date.now() - startTime) / 1000)
    if (seconds < 60) return `${seconds}s ago`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    return `${hours}h ${minutes % 60}m ago`
  }

  const formatDuration = (startTime: number | null, endTime: number | null) => {
    if (!startTime) return 'N/A'
    const end = endTime || Date.now()
    const seconds = Math.floor((end - startTime) / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ${seconds % 60}s`
    const hours = Math.floor(minutes / 60)
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`
  }

  const formatDateTime = (timestamp: number | null) => {
    if (!timestamp) return 'N/A'
    return new Date(timestamp).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
  }

  const loadMetadata = async () => {
    try {
      const [dataTypesData, strategiesData] = await Promise.all([
        metadataApi.getDataTypes(),
        metadataApi.getStrategies(),
      ])
      setDataTypes(dataTypesData)
      setStrategies(strategiesData)
    } catch (error) {
      console.error('Failed to load metadata:', error)
      setError('Failed to load metadata')
    }
  }

  const handleNext = () => {
    setActiveStep((prev) => Math.min(prev + 1, steps.length - 1))
  }

  const handleBack = () => {
    setActiveStep((prev) => Math.max(prev - 1, 0))
  }

  const handleStrategyToggle = (strategyName: string) => {
    setSelectedStrategies((prev) =>
      prev.includes(strategyName)
        ? prev.filter((s) => s !== strategyName)
        : [...prev, strategyName]
    )
  }

  const addDataSource = () => {
    setDataSources((prev) => [...prev, { id: crypto.randomUUID(), config: {} }])
  }

  const removeDataSource = (id: string) => {
    setDataSources((prev) => prev.filter((source) => source.id !== id))
  }

  const updateDataSource = (id: string, field: string, value: any) => {
    setDataSources((prev) =>
      prev.map((source) =>
        source.id === id
          ? { ...source, config: { ...source.config, [field]: value } }
          : source
      )
    )
  }

  const handleSubmit = async () => {
    if (!selectedProjectId) {
      setError('Please select a project first')
      return
    }

    setIsSubmitting(true)
    setError('')

    try {
      // Combine multiple data sources based on data type
      let dataConfig: Record<string, any> = {}

      if (selectedDataType === 'text') {
        // Text type: combine into text_entries array
        dataConfig = {
          text_entries: dataSources.map((source) => source.config)
        }
      } else if (selectedDataType === 'delta_table') {
        // Delta table: combine into tables array
        dataConfig = {
          tables: dataSources.map((source) => source.config)
        }
      } else if (['csv', 'json', 'pdf'].includes(selectedDataType)) {
        // File types: Upload files to UC Volume first, then pass volume_path
        // This is a scalable approach - files are stored in Unity Catalog Volumes
        // instead of being passed as job parameters
        
        if (selectedFiles.length === 0) {
          setError('Please select files to upload')
          setIsSubmitting(false)
          return
        }
        
        try {
          const volumePath = await uploadFilesToVolume()
          if (!volumePath) {
            setError('Failed to upload files to volume')
            setIsSubmitting(false)
            return
          }
          
          // Pass volume_path instead of file content
          // The build job will read files from this volume path
          dataConfig = {
            volume_path: volumePath,
            file_pattern: `*.${selectedDataType}`,
            // Include additional config from first data source
            ...dataSources[0]?.config,
            // Ensure volume_path is set
            volume_path: volumePath,
          }
          
          console.log(`[INFO] Files uploaded to volume: ${volumePath}`)
          
        } catch (uploadError) {
          console.error('Failed to upload files:', uploadError)
          setError('Failed to upload files to Unity Catalog Volume. Please try again.')
          setIsSubmitting(false)
          return
        }
      } else if (selectedDataType === 'uc_volume') {
        // UC Volume: user provides volume path directly
        dataConfig = dataSources[0]?.config || {}
      } else {
        // Other types: use first source's config (backward compatibility)
        dataConfig = dataSources[0]?.config || {}
      }

      const config = {
        data_type: selectedDataType,
        data_config: dataConfig,
        strategies: selectedStrategies.reduce((acc, s) => {
          acc[s] = {}
          return acc
        }, {} as Record<string, any>),
        embedding_model_endpoint: embeddingEndpoint,
        vs_endpoint_name: vsEndpoint,
        create_index: true,
      }

      const result = await buildsApi.create({
        project_id: selectedProjectId,
        config,
      })

      // Store run ID and initial status
      setSubmittedRunId(result.run_id)
      // Fetch initial status with full details
      try {
        const status = await buildsApi.getStatus(result.run_id)
        setJobStatus({
          state: status.state,
          job_url: status.job_url,
          start_time: status.start_time,
          status: status.status,
          run_id: status.run_id,
        })
      } catch (error) {
        // Fallback to basic info if status fetch fails
        setJobStatus({
          state: result.state,
          job_url: result.job_url || null,
          start_time: new Date(result.created_at).getTime(),
          status: null,
          run_id: result.run_id,
        })
      }

      // Reset form (but keep showing status)
      setActiveStep(0)
      setSelectedDataType('')
      setSelectedStrategies([])
      setDataSources([{ id: crypto.randomUUID(), config: {} }])
      setEmbeddingEndpoint('')
      setVsEndpoint('')
      // Reset file upload state
      setSelectedFiles([])
      setUploadedVolumePath(null)
      setUploadProgress(null)
    } catch (error) {
      console.error('Failed to submit build job:', error)
      setError('Failed to submit build job')
      setIsSubmitting(false)
    }
  }

  const selectedDataTypeInfo = dataTypes.find((dt) => dt.name === selectedDataType)

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <div className="space-y-4">
            <Select
              label="Data Type"
              value={selectedDataType}
              onChange={(e) => setSelectedDataType(e.target.value)}
              options={[
                { value: '', label: 'Select a data type' },
                ...dataTypes.map((dt) => ({
                  value: dt.name,
                  label: dt.display_name,
                })),
              ]}
              required
            />
            {selectedDataTypeInfo && (
              <div className="p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200">
                <p className="text-sm text-databricks-gray-600">
                  <span className="font-medium">Compatible strategies:</span>{' '}
                  {selectedDataTypeInfo.compatible_strategies.join(', ')}
                </p>
              </div>
            )}
          </div>
        )

      case 1:
        return (
          <div className="space-y-4">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-medium text-databricks-gray-900">
                Configure Data Source{dataSources.length > 1 ? 's' : ''}
              </h3>
              <Badge variant="secondary">{dataSources.length} source{dataSources.length > 1 ? 's' : ''}</Badge>
            </div>

            {/* Render each data source as a card */}
            {dataSources.map((source, index) => (
              <Card key={source.id} className="p-4 border-2 border-databricks-gray-200">
                <div className="flex items-center justify-between mb-4">
                  <h4 className="text-md font-medium text-databricks-gray-800">
                    Source {index + 1}
                  </h4>
                  {dataSources.length > 1 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeDataSource(source.id)}
                      className="text-databricks-error hover:bg-red-50"
                    >
                      <Trash2 className="w-4 h-4 mr-1" />
                      Remove
                    </Button>
                  )}
                </div>

                <div className="space-y-4">
                  {selectedDataTypeInfo?.input_schema?.fields?.map((field: any) => {
                    if (field.type === 'textarea') {
                      return (
                        <div key={field.name}>
                          <label className="block text-sm font-medium text-databricks-gray-700 mb-1">
                            {field.label}
                            {field.required && <span className="text-databricks-error ml-1">*</span>}
                          </label>
                          <textarea
                            value={source.config[field.name] || ''}
                            onChange={(e) => updateDataSource(source.id, field.name, e.target.value)}
                            placeholder={field.default || ''}
                            required={field.required}
                            rows={6}
                            className="w-full px-3 py-2 border border-databricks-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-blue focus:border-databricks-blue"
                          />
                        </div>
                      )
                    } else if (field.type === 'bool') {
                      return (
                        <div key={field.name} className="flex items-center">
                          <input
                            type="checkbox"
                            checked={source.config[field.name] || field.default || false}
                            onChange={(e) => updateDataSource(source.id, field.name, e.target.checked)}
                            className="h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue"
                          />
                          <label className="ml-2 text-sm text-databricks-gray-700">
                            {field.label}
                          </label>
                        </div>
                      )
                    } else {
                      return (
                        <Input
                          key={field.name}
                          label={field.label}
                          placeholder={field.default || ''}
                          required={field.required}
                          onChange={(e) => updateDataSource(source.id, field.name, e.target.value)}
                          value={source.config[field.name] || ''}
                        />
                      )
                    }
                  })}
                </div>
              </Card>
            ))}

            {/* Add Another Source Button - only for non-file types */}
            {!isFileUploadDataType && (
              <Button
                variant="outline"
                onClick={addDataSource}
                className="w-full border-2 border-dashed border-databricks-gray-300 hover:border-databricks-blue hover:bg-blue-50"
              >
                <Plus className="w-4 h-4 mr-2" />
                Add Another {selectedDataTypeInfo?.display_name || 'Source'}
              </Button>
            )}

            {/* File Upload Section for file-based data types */}
            {isFileUploadDataType && (
              <div className="space-y-4">
                <h4 className="text-md font-medium text-databricks-gray-800">
                  Upload Files
                </h4>
                
                {/* Drag and Drop Zone */}
                <div
                  onDrop={handleDrop}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onClick={() => fileInputRef.current?.click()}
                  className={`
                    relative border-2 border-dashed rounded-lg p-8 text-center cursor-pointer
                    transition-all duration-200
                    ${isDragOver 
                      ? 'border-databricks-blue bg-blue-50' 
                      : 'border-databricks-gray-300 hover:border-databricks-blue hover:bg-blue-50'
                    }
                  `}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    multiple
                    accept={selectedDataType === 'pdf' ? '.pdf' : selectedDataType === 'csv' ? '.csv' : '.json'}
                    onChange={(e) => handleFileSelect(e.target.files)}
                    className="hidden"
                  />
                  
                  <Upload className={`w-10 h-10 mx-auto mb-3 ${isDragOver ? 'text-databricks-blue' : 'text-databricks-gray-400'}`} />
                  
                  <p className="text-sm font-medium text-databricks-gray-700">
                    {isDragOver ? 'Drop files here' : 'Drag and drop files here'}
                  </p>
                  <p className="text-xs text-databricks-gray-500 mt-1">
                    or click to browse
                  </p>
                  <p className="text-xs text-databricks-gray-400 mt-2">
                    Supported: {selectedDataType === 'pdf' ? 'PDF' : selectedDataType === 'csv' ? 'CSV' : 'JSON'} files
                  </p>
                </div>

                {/* Selected Files List */}
                {selectedFiles.length > 0 && (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium text-databricks-gray-700">
                        {selectedFiles.length} file{selectedFiles.length > 1 ? 's' : ''} selected
                      </span>
                      {uploadedVolumePath && (
                        <Badge variant="success">
                          <CheckCircle className="w-3 h-3 mr-1" />
                          Uploaded
                        </Badge>
                      )}
                    </div>
                    
                    <div className="max-h-48 overflow-y-auto space-y-2">
                      {selectedFiles.map((fileState, index) => (
                        <div
                          key={index}
                          className="flex items-center justify-between p-3 bg-databricks-gray-50 rounded-md border border-databricks-gray-200"
                        >
                          <div className="flex items-center space-x-3 flex-1 min-w-0">
                            <File className="w-4 h-4 text-databricks-gray-500 flex-shrink-0" />
                            <div className="flex-1 min-w-0">
                              <p className="text-sm font-medium text-databricks-gray-800 truncate">
                                {fileState.file.name}
                              </p>
                              <p className="text-xs text-databricks-gray-500">
                                {(fileState.file.size / 1024).toFixed(1)} KB
                              </p>
                            </div>
                            {fileState.status === 'uploaded' && (
                              <CheckCircle className="w-4 h-4 text-green-500 flex-shrink-0" />
                            )}
                            {fileState.status === 'uploading' && (
                              <Loader2 className="w-4 h-4 text-databricks-blue animate-spin flex-shrink-0" />
                            )}
                            {fileState.status === 'error' && (
                              <AlertCircle className="w-4 h-4 text-red-500 flex-shrink-0" />
                            )}
                          </div>
                          <button
                            onClick={() => removeFile(index)}
                            className="ml-2 p-1 text-databricks-gray-400 hover:text-databricks-error transition-colors"
                            disabled={isUploading}
                          >
                            <X className="w-4 h-4" />
                          </button>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Upload Progress */}
                {isUploading && uploadProgress && (
                  <div className="p-4 bg-blue-50 border border-blue-200 rounded-md">
                    <div className="flex items-center space-x-3">
                      <Loader2 className="w-5 h-5 text-databricks-blue animate-spin" />
                      <div className="flex-1">
                        <p className="text-sm font-medium text-blue-800">
                          Uploading files to Unity Catalog Volume...
                        </p>
                        <p className="text-xs text-blue-600">
                          {uploadProgress.current} of {uploadProgress.total} files
                        </p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Volume Path Info */}
                {uploadedVolumePath && (
                  <div className="p-4 bg-green-50 border border-green-200 rounded-md">
                    <div className="flex items-start space-x-3">
                      <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-green-800">
                          Files uploaded successfully
                        </p>
                        <p className="text-xs text-green-600 truncate mt-1">
                          Volume path: {uploadedVolumePath}
                        </p>
                      </div>
                    </div>
                  </div>
                )}

                {/* Info Box */}
                <div className="p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200">
                  <p className="text-sm text-databricks-gray-600">
                    <span className="font-medium">Scalable upload:</span> Files are uploaded to Unity Catalog Volumes 
                    for efficient processing. The build job reads directly from the volume, 
                    enabling large-scale data handling.
                  </p>
                </div>
              </div>
            )}
          </div>
        )

      case 2:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Select Chunking Strategies
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {strategies
                .filter(
                  (s) =>
                    !selectedDataTypeInfo ||
                    selectedDataTypeInfo.compatible_strategies.includes(s.name)
                )
                .map((strategy) => (
                  <Card
                    key={strategy.name}
                    className={`cursor-pointer transition-all ${
                      selectedStrategies.includes(strategy.name)
                        ? 'ring-2 ring-databricks-blue bg-blue-50'
                        : 'hover:shadow-db-md'
                    }`}
                    padding={false}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start">
                        <input
                          type="checkbox"
                          checked={selectedStrategies.includes(strategy.name)}
                          onChange={() => handleStrategyToggle(strategy.name)}
                          className="mt-1 h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue"
                        />
                        <div className="ml-3 flex-1">
                          <label className="font-medium text-databricks-gray-900 cursor-pointer">
                            {strategy.display_name}
                          </label>
                          <p className="text-sm text-databricks-gray-600 mt-1">
                            {strategy.description}
                          </p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
            </div>
          </div>
        )

      case 3:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Configure Endpoints
            </h3>
            <Input
              label="Embedding Model Endpoint"
              value={embeddingEndpoint}
              onChange={(e) => setEmbeddingEndpoint(e.target.value)}
              helperText="Databricks Model Serving endpoint for embeddings"
              placeholder="e.g., databricks-bge-large-en"
              required
            />
            <Input
              label="Vector Search Endpoint"
              value={vsEndpoint}
              onChange={(e) => setVsEndpoint(e.target.value)}
              helperText="Databricks Vector Search endpoint name"
              placeholder="e.g., vs-endpoint-default"
              required
            />
          </div>
        )

      default:
        return null
    }
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Build Retrieval Pipeline</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">
          Configure and submit build jobs for your retrieval pipeline
        </p>
      </div>

      {!selectedProjectId ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-yellow-100 rounded-full flex items-center justify-center">
              <Clock className="w-8 h-8 text-yellow-600" />
            </div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">
              No Project Selected
            </h3>
            <p className="text-sm text-databricks-gray-600 mb-6 max-w-md mx-auto">
              You need to select or create a project before you can create a build.
              Please go to the Projects page and select a project to continue.
            </p>
            <Button
              variant="primary"
              onClick={() => navigate('/projects')}
            >
              Go to Projects
            </Button>
          </div>
        </Card>
      ) : (
        <>
          {selectedProject && (
            <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
              <p className="text-sm text-blue-900">
                <span className="font-medium">Current project:</span> {selectedProject.project_name}
              </p>
            </div>
          )}

          <Card>
        {/* Progress Steps */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            {steps.map((step, index) => (
              <div key={step.number} className="flex items-center flex-1">
                <div className="flex items-center">
                  <div
                    className={`flex items-center justify-center w-10 h-10 rounded-full border-2 font-medium text-sm transition-colors ${
                      index < activeStep
                        ? 'bg-databricks-blue border-databricks-blue text-white'
                        : index === activeStep
                        ? 'border-databricks-blue text-databricks-blue bg-white'
                        : 'border-databricks-gray-300 text-databricks-gray-500 bg-white'
                    }`}
                  >
                    {index < activeStep ? (
                      <CheckCircle2 className="w-5 h-5" />
                    ) : (
                      step.number
                    )}
                  </div>
                  <div className="ml-3">
                    <p
                      className={`text-sm font-medium ${
                        index <= activeStep
                          ? 'text-databricks-gray-900'
                          : 'text-databricks-gray-500'
                      }`}
                    >
                      {step.label}
                    </p>
                  </div>
                </div>
                {index < steps.length - 1 && (
                  <div
                    className={`flex-1 h-0.5 mx-4 ${
                      index < activeStep ? 'bg-databricks-blue' : 'bg-databricks-gray-300'
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Step Content */}
        <div className="min-h-[300px] mb-8">{renderStepContent(activeStep)}</div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-800">{error}</p>
          </div>
        )}

        {/* Navigation Buttons */}
        <div className="flex justify-between items-center pt-6 border-t border-databricks-gray-200">
          <Button
            variant="outline"
            onClick={handleBack}
            disabled={activeStep === 0}
            icon={<ChevronLeft className="w-4 h-4" />}
          >
            Back
          </Button>

          <div className="flex gap-3">
            {activeStep === steps.length - 1 ? (
              <Button
                variant="primary"
                onClick={handleSubmit}
                isLoading={isSubmitting}
                disabled={!selectedProjectId || isSubmitting}
                icon={<PlayCircle className="w-4 h-4" />}
              >
                Submit Build Job
              </Button>
            ) : (
              <Button
                variant="primary"
                onClick={handleNext}
                icon={<ChevronRight className="w-4 h-4" />}
              >
                Next
              </Button>
            )}
          </div>
        </div>
      </Card>

      {/* Job Status Block - Below the form */}
      {jobStatus && submittedRunId && (
        <Card className="mt-6">
          <CardContent className="p-6">
            <div className="flex items-start justify-between mb-6">
              <h2 className="text-lg font-semibold text-databricks-gray-900">
                Build Job Status
              </h2>
              <Badge
                variant={
                  jobStatus.state === 'SUCCESS'
                    ? 'success'
                    : jobStatus.state === 'FAILED'
                    ? 'error'
                    : jobStatus.state === 'RUNNING'
                    ? 'info'
                    : 'warning'
                }
              >
                {jobStatus.state === 'SUCCESS' && <CheckCircle className="w-3 h-3 mr-1" />}
                {jobStatus.state}
              </Badge>
            </div>

            {/* Job Details Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
              <div className="space-y-3">
                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Job ID</label>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-sm font-mono text-databricks-gray-900">
                      {jobStatus.status?.job_id || 'N/A'}
                    </span>
                    {jobStatus.status?.job_id && (
                      <button
                        onClick={() => copyToClipboard(String(jobStatus.status.job_id))}
                        className="text-databricks-gray-400 hover:text-databricks-gray-600"
                        title="Copy Job ID"
                      >
                        <Copy className="w-3 h-3" />
                      </button>
                    )}
                  </div>
                </div>

                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Job run ID</label>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-sm font-mono text-databricks-gray-900">
                      {jobStatus.status?.run_id || 'N/A'}
                    </span>
                    {jobStatus.status?.run_id && (
                      <button
                        onClick={() => copyToClipboard(String(jobStatus.status.run_id))}
                        className="text-databricks-gray-400 hover:text-databricks-gray-600"
                        title="Copy Job Run ID"
                      >
                        <Copy className="w-3 h-3" />
                      </button>
                    )}
                  </div>
                </div>

                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Task run ID</label>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-sm font-mono text-databricks-gray-900">
                      {jobStatus.status?.task_run_id || 'N/A'}
                    </span>
                    {jobStatus.status?.task_run_id && (
                      <button
                        onClick={() => copyToClipboard(String(jobStatus.status.task_run_id))}
                        className="text-databricks-gray-400 hover:text-databricks-gray-600"
                        title="Copy Task Run ID"
                      >
                        <Copy className="w-3 h-3" />
                      </button>
                    )}
                  </div>
                </div>

                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Run as</label>
                  <p className="text-sm text-databricks-gray-900 mt-1">
                    {jobStatus.status?.run_as || 'N/A'}
                  </p>
                </div>
              </div>

              <div className="space-y-3">
                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Started</label>
                  <p className="text-sm text-databricks-gray-900 mt-1">
                    {formatDateTime(jobStatus.start_time)}
                  </p>
                </div>

                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Ended</label>
                  <p className="text-sm text-databricks-gray-900 mt-1">
                    {formatDateTime(jobStatus.status?.end_time)}
                  </p>
                </div>

                <div>
                  <label className="text-xs font-medium text-databricks-gray-500 uppercase">Duration</label>
                  <div className="mt-1">
                    <p className="text-sm text-databricks-gray-900">
                      {formatDuration(jobStatus.start_time, jobStatus.status?.end_time)}
                    </p>
                    {jobStatus.start_time && (
                      <div className="w-full bg-databricks-gray-200 rounded-full h-1.5 mt-2">
                        <div
                          className={`h-1.5 rounded-full ${
                            jobStatus.state === 'SUCCESS'
                              ? 'bg-green-500'
                              : jobStatus.state === 'FAILED'
                              ? 'bg-red-500'
                              : 'bg-databricks-blue'
                          }`}
                          style={{
                            width: jobStatus.status?.end_time
                              ? '100%'
                              : `${Math.min(90, (Date.now() - jobStatus.start_time) / 1000 / 60)}%`,
                          }}
                        />
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {jobStatus.job_url && (
              <div className="pt-4 border-t border-databricks-gray-200">
                <a
                  href={jobStatus.job_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center text-sm text-databricks-blue hover:underline"
                >
                  <ExternalLink className="w-4 h-4 mr-1" />
                  View Job in Databricks
                </a>
              </div>
            )}

            {jobStatus.state === 'SUCCESS' && (
              <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-md">
                <p className="text-sm text-green-800">
                  Build job completed successfully! Redirecting to Evaluate page...
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      )}
        </>
      )}
    </div>
  )
}
