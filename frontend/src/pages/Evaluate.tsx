import { useState, useEffect, useRef } from 'react'
import { PlayCircle, ExternalLink, Clock } from 'lucide-react'
import { evaluationsApi } from '../services/evaluations'
import { buildsApi } from '../services/builds'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { BuildJob } from '../types'

type DatasetType = 'delta_table' | 'csv' | 'excel'

export default function Evaluate() {
  const { selectedProject, selectedProjectId } = useProject()
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [datasetType, setDatasetType] = useState<DatasetType>('delta_table')
  const [datasetPath, setDatasetPath] = useState('')
  const [topK, setTopK] = useState(10)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [submittedRunId, setSubmittedRunId] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<{
    state: string
    job_url: string | null
    start_time: number | null
  } | null>(null)
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null)

  useEffect(() => {
    if (selectedProjectId) {
      loadLatestBuild()
    }
    
    // Cleanup polling on unmount
    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current)
      }
    }
  }, [selectedProjectId])

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
      const status = await evaluationsApi.getStatus(submittedRunId)
      setJobStatus({
        state: status.state,
        job_url: status.job_url,
        start_time: status.start_time,
      })
      
      // Stop polling if job completed or failed
      if (status.state === 'SUCCESS' || status.state === 'FAILED') {
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

  const loadLatestBuild = async () => {
    if (!selectedProjectId) return
    
    setIsLoading(true)
    setError('') // Clear any previous errors
    try {
      const buildsData = await buildsApi.getByProject(selectedProjectId)
      // Find the most recent SUCCESS build
      const successfulBuilds = buildsData.filter((b: BuildJob) => b.state === 'SUCCESS')
      
      if (successfulBuilds.length === 0) {
        setError('No successful build runs found. Please complete a build first.')
        setSelectedRunId(null)
        return
      }
      
      // Sort by creation date (most recent first)
      const sortedBuilds = successfulBuilds.sort(
        (a: BuildJob, b: BuildJob) => 
          new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      )
      
      // Automatically select the most recent successful build
      setSelectedRunId(sortedBuilds[0].run_id)
    } catch (error: any) {
      console.error('Failed to load builds:', error)
      setError(error?.response?.data?.detail || 'Failed to load builds')
      setSelectedRunId(null)
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = async () => {
    if (!selectedRunId || !datasetPath) {
      setError('Please fill in all required fields')
      return
    }

    setIsSubmitting(true)
    setError('')

    try {
      // For delta_table, use the path directly as queries_table
      // For CSV/Excel, we'd need to upload and create a table first
      // For now, we'll assume the user provides a delta table path
      const queriesTable = datasetType === 'delta_table' 
        ? datasetPath 
        : datasetPath // TODO: Handle CSV/Excel upload and table creation

      const result = await evaluationsApi.create({
        run_id: selectedRunId,
        queries_table: queriesTable,
        dataset_type: datasetType,
        top_k: topK,
      })

      // Store run ID and initial status
      setSubmittedRunId(result.run_id)
      setJobStatus({
        state: result.state,
        job_url: result.job_url || null,
        start_time: new Date(result.created_at).getTime(),
      })

      // Reset form (but keep showing status)
      setDatasetPath('')
    } catch (error: any) {
      console.error('Failed to submit evaluation:', error)
      setError(error?.response?.data?.detail || 'Failed to submit evaluation job')
      setIsSubmitting(false)
    }
  }


  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Evaluate Pipeline</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">
          Submit evaluation jobs to test your retrieval pipeline performance
        </p>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to start evaluation.
          </p>
        </div>
      )}

      {selectedProject && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
          <p className="text-sm text-blue-900">
            <span className="font-medium">Current project:</span> {selectedProject.project_name}
          </p>
        </div>
      )}

      {jobStatus && submittedRunId && (
        <Card className="mb-6">
          <CardContent className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h2 className="text-lg font-semibold text-databricks-gray-900 mb-2">
                  Evaluation Job Status
                </h2>
                <p className="text-sm text-databricks-gray-600 font-mono">
                  Run ID: {submittedRunId.substring(0, 12)}...
                </p>
              </div>
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
                {jobStatus.state}
              </Badge>
            </div>

            {jobStatus.job_url && (
              <div className="mb-4">
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

            <div className="flex items-center text-sm text-databricks-gray-600">
              <Clock className="w-4 h-4 mr-2" />
              <span>Time since execution: {formatTimeSince(jobStatus.start_time)}</span>
            </div>

            {jobStatus.state === 'SUCCESS' && (
              <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-md">
                <p className="text-sm text-green-800">
                  Evaluation job completed successfully!
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Submit Evaluation Form */}
      <Card>
        <h2 className="text-lg font-semibold text-databricks-gray-900 mb-6">
          Submit Evaluation
        </h2>

        <div className="space-y-4">
          {selectedRunId ? (
            <div className="p-4 bg-blue-50 border border-blue-200 rounded-md">
              <p className="text-sm text-blue-900">
                <span className="font-medium">Using build run:</span>{' '}
                <span className="font-mono">{selectedRunId.substring(0, 12)}...</span>
              </p>
              <p className="text-xs text-blue-700 mt-1">
                Automatically selected the most recent successful build for this project
              </p>
            </div>
          ) : (
            <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-md">
              <p className="text-sm text-yellow-800">
                No successful build runs found. Please complete a build first.
              </p>
            </div>
          )}

          <Select
              label="Dataset Type"
              value={datasetType}
              onChange={(e) => {
                setDatasetType(e.target.value as DatasetType)
                setDatasetPath('')
              }}
              options={[
                { value: 'delta_table', label: 'Delta Table' },
                { value: 'csv', label: 'CSV File' },
                { value: 'excel', label: 'Excel File' },
              ]}
              required
              helperText="Select the type of golden dataset"
            />

            <Input
              label={
                datasetType === 'delta_table'
                  ? 'Delta Table Path'
                  : datasetType === 'csv'
                  ? 'CSV File Path'
                  : 'Excel File Path'
              }
              value={datasetPath}
              onChange={(e) => setDatasetPath(e.target.value)}
              placeholder={
                datasetType === 'delta_table'
                  ? 'e.g., catalog.schema.queries_table'
                  : datasetType === 'csv'
                  ? 'e.g., /path/to/file.csv'
                  : 'e.g., /path/to/file.xlsx'
              }
              required
              helperText={
                datasetType === 'delta_table'
                  ? 'Fully qualified table name (catalog.schema.table)'
                  : 'Path to the file in Databricks workspace or DBFS'
              }
            />

            <Input
              label="Top K"
              type="number"
              value={topK.toString()}
              onChange={(e) => setTopK(parseInt(e.target.value) || 10)}
              placeholder="10"
              helperText="Number of top results to retrieve"
            />

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-md">
                <p className="text-sm text-red-800">{error}</p>
              </div>
            )}

            <Button
              variant="primary"
              onClick={handleSubmit}
              isLoading={isSubmitting}
              disabled={!selectedProjectId || !selectedRunId || !datasetPath || isSubmitting}
              icon={<PlayCircle className="w-4 h-4" />}
              className="w-full"
            >
              Submit Evaluation Job
            </Button>
          </div>
        </Card>

      {/* Info Box */}
      <Card className="mt-6 bg-databricks-gray-50">
        <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
          Evaluation Requirements
        </h3>
        <ul className="space-y-2 text-sm text-databricks-gray-700">
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              Your dataset must contain a <code className="bg-gray-100 px-1 rounded">query_text</code> column
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              Optionally include an <code className="bg-gray-100 px-1 rounded">expected_chunks</code> column with JSON array of chunk IDs for labeled evaluation
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>The evaluation will test Recall@K and NDCG@K metrics</span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>Results will be logged to MLflow and viewable in the Review section</span>
          </li>
        </ul>
      </Card>
    </div>
  )
}
