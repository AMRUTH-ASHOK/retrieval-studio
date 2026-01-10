import { useState, useEffect, useRef } from 'react'
import { PlayCircle, ChevronRight, ChevronLeft, CheckCircle2, ExternalLink, Clock, Copy, CheckCircle } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { metadataApi } from '../services/metadata'
import { DataType, Strategy } from '../types'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { useNavigate } from 'react-router-dom'

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
  const [dataConfig, setDataConfig] = useState<Record<string, any>>({})
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
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

  const handleSubmit = async () => {
    if (!selectedProjectId) {
      setError('Please select a project first')
      return
    }

    setIsSubmitting(true)
    setError('')

    try {
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
      setDataConfig({})
      setEmbeddingEndpoint('')
      setVsEndpoint('')
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
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Configure Data Source
            </h3>
            {selectedDataTypeInfo?.input_schema?.fields?.map((field: any) => {
              if (field.type === 'textarea') {
                return (
                  <div key={field.name}>
                    <label className="block text-sm font-medium text-databricks-gray-700 mb-1">
                      {field.label}
                      {field.required && <span className="text-databricks-error ml-1">*</span>}
                    </label>
                    <textarea
                      value={dataConfig[field.name] || ''}
                      onChange={(e) =>
                        setDataConfig((prev) => ({ ...prev, [field.name]: e.target.value }))
                      }
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
                      checked={dataConfig[field.name] || field.default || false}
                      onChange={(e) =>
                        setDataConfig((prev) => ({ ...prev, [field.name]: e.target.checked }))
                      }
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
                    onChange={(e) =>
                      setDataConfig((prev) => ({ ...prev, [field.name]: e.target.value }))
                    }
                    value={dataConfig[field.name] || ''}
                  />
                )
              }
            })}
            {selectedDataTypeInfo && selectedDataTypeInfo.input_schema?.source_type === 'upload' && (
              <div className="p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200">
                <p className="text-sm text-databricks-gray-600">
                  <span className="font-medium">Note:</span> File upload is handled through Databricks workspace.
                  Please upload your files to a UC Volume or use Delta Table as data source.
                </p>
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
