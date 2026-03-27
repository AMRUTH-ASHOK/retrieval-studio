import { useState, useEffect, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { PlayCircle, ExternalLink, Clock } from 'lucide-react'
import { evaluationsApi } from '../services/evaluations'
import { buildsApi } from '../services/builds'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { BuildJob } from '../types'

export default function Evaluate() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const { selectedProject, selectedProjectId } = useProject()
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)
  const [topK, setTopK] = useState(10)
  const [compareQueryTypes, setCompareQueryTypes] = useState(false)
  const [existingGoldenTable, setExistingGoldenTable] = useState('')
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
  const statusCardRef = useRef<HTMLDivElement | null>(null)
  const [evaluationMode, setEvaluationMode] = useState<'existing' | 'auto-generate'>('existing')
  const [numQueries, setNumQueries] = useState(50)
  const [queryStyle, setQueryStyle] = useState('keyword')

  useEffect(() => {
    if (selectedProjectId) {
      const buildRunId = searchParams.get('buildRunId')
      if (buildRunId) {
        setSelectedRunId(buildRunId)
      } else {
        loadLatestBuild()
      }
    }

    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current)
      }
    }
  }, [selectedProjectId, searchParams])

  // Auto-scroll to status card when it appears
  useEffect(() => {
    if (jobStatus && submittedRunId && statusCardRef.current) {
      statusCardRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [jobStatus, submittedRunId])

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

      if (status.state === 'SUCCESS' || status.state === 'FAILED') {
        if (pollingIntervalRef.current) {
          clearInterval(pollingIntervalRef.current)
          pollingIntervalRef.current = null
        }

        if (status.state === 'SUCCESS') {
          setTimeout(() => {
            navigate('/review')
          }, 2000)
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
    if (!selectedRunId) {
      setError('Please select a build run')
      return
    }

    const isAutoGenerate = evaluationMode === 'auto-generate'

    if (evaluationMode === 'existing' && !existingGoldenTable) {
      setError('Please provide the golden dataset table')
      return
    }

    setIsSubmitting(true)
    setError('')

    try {
      const submitData: any = {
        run_id: selectedRunId,
        top_k: topK,
        compare_query_types: compareQueryTypes,
      }

      if (isAutoGenerate) {
        submitData.auto_generate_queries = true
        submitData.generate_golden_dataset = true
        submitData.num_queries = numQueries
        submitData.query_style = queryStyle
      } else {
        submitData.use_golden_dataset = true
        submitData.golden_dataset_table = existingGoldenTable
      }

      const result = await evaluationsApi.create(submitData)

      console.log('[DEBUG] Evaluation submitted:', result)
      console.log('[DEBUG] Setting submittedRunId to:', result.run_id)

      // Store run ID and initial status
      setSubmittedRunId(result.run_id)

      // Fetch initial status with full details (same pattern as Build.tsx)
      let finalStatus
      try {
        console.log('[DEBUG] Fetching full status for:', result.run_id)
        const status = await evaluationsApi.getStatus(result.run_id)
        console.log('[DEBUG] Got status from API:', status)
        finalStatus = {
          state: status.state,
          job_url: status.job_url,
          start_time: status.start_time,
        }
        setJobStatus(finalStatus)
        console.log('[DEBUG] Set jobStatus to:', finalStatus)
      } catch (error) {
        // Fallback to basic info if status fetch fails
        console.log('[DEBUG] Status fetch failed, using fallback:', error)
        finalStatus = {
          state: result.state,
          job_url: result.job_url || null,
          start_time: new Date(result.created_at).getTime(),
        }
        setJobStatus(finalStatus)
        console.log('[DEBUG] Set jobStatus to (fallback):', finalStatus)
      }

      console.log('[DEBUG] About to reset form and set isSubmitting to false')

      // Reset form (but keep showing status)
      setIsSubmitting(false)
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

      {!selectedProjectId ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-yellow-100 rounded-full flex items-center justify-center">
              <Clock className="w-4 h-4 text-yellow-600" />
            </div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">
              No Project Selected
            </h3>
            <p className="text-sm text-databricks-gray-600 mb-6 max-w-md mx-auto">
              You need to select a project and complete a build before you can run evaluations.
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

          {/* Evaluation Mode Selection */}
          <div className="space-y-6">
            <div>
              <label className="block text-sm font-medium text-databricks-gray-900 mb-3">
                Select Evaluation Mode
              </label>
              <div className="space-y-4">
                <label className="flex items-start p-5 border-2 rounded-lg cursor-pointer hover:bg-gray-50 transition-all shadow-sm"
                  style={{
                    borderColor: evaluationMode === 'existing' ? '#2196F3' : '#E5E7EB',
                    backgroundColor: evaluationMode === 'existing' ? '#F0F9FF' : 'white'
                  }}>
                  <input
                    type="radio"
                    name="evaluationMode"
                    checked={evaluationMode === 'existing'}
                    onChange={() => setEvaluationMode('existing')}
                    className="mt-1 w-4 h-4 text-databricks-blue border-gray-300 focus:ring-databricks-blue"
                  />
                  <div className="ml-3 flex-1">
                    <div className="font-semibold text-databricks-gray-900 text-base">
                      Option 1: Use Existing Golden Dataset
                    </div>
                    <p className="text-sm text-databricks-gray-600 mt-1">
                      Provide a Delta table with <code className="bg-gray-100 px-1 rounded">query_text</code> and
                      <code className="bg-gray-100 px-1 rounded ml-1">expected_chunks</code> (chunk text only).
                    </p>
                  </div>
                </label>

                {/* Visual Separator */}
                <div className="flex items-center gap-3">
                  <div className="flex-1 border-t border-gray-300"></div>
                  <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">OR</span>
                  <div className="flex-1 border-t border-gray-300"></div>
                </div>

                <label className="flex items-start p-5 border-2 rounded-lg cursor-pointer hover:bg-gray-50 transition-all shadow-sm"
                  style={{
                    borderColor: evaluationMode === 'auto-generate' ? '#2196F3' : '#E5E7EB',
                    backgroundColor: evaluationMode === 'auto-generate' ? '#F0F9FF' : 'white'
                  }}>
                  <input
                    type="radio"
                    name="evaluationMode"
                    checked={evaluationMode === 'auto-generate'}
                    onChange={() => setEvaluationMode('auto-generate')}
                    className="mt-1 w-4 h-4 text-databricks-blue border-gray-300 focus:ring-databricks-blue"
                  />
                  <div className="ml-3 flex-1">
                    <div className="font-semibold text-databricks-gray-900 text-base">
                      Option 2: Auto-Generate Queries
                    </div>
                    <p className="text-sm text-databricks-gray-600 mt-1">
                      An LLM generates evaluation queries by reading your build's chunk tables. For per-source builds, samples from ALL source tables proportionally.
                    </p>
                  </div>
                </label>
              </div>
            </div>

            {/* Configuration Section */}
            <div className="border-t-4 border-databricks-blue pt-6">
              <h3 className="text-md font-semibold text-databricks-gray-900 mb-4">
                {evaluationMode === 'existing' ? 'Golden Dataset Configuration' : 'Query Generation Configuration'}
              </h3>

              {evaluationMode === 'existing' ? (
                <div className="space-y-4 p-5 bg-blue-50 border border-blue-200 rounded-lg">
                  <Input
                    label="Golden Dataset Table (Delta)"
                    value={existingGoldenTable}
                    onChange={(e) => setExistingGoldenTable(e.target.value)}
                    placeholder="catalog.schema.rs_golden_project"
                    helperText="Must include query_text and expected_chunks columns"
                  />
                </div>
              ) : (
                <div className="space-y-4 p-5 bg-green-50 border border-green-200 rounded-lg">
                  <p className="text-xs text-green-700 mb-2">
                    The system will auto-generate queries from your build's chunk tables and store the golden dataset automatically.
                  </p>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-databricks-gray-700 mb-1">Number of Queries</label>
                      <input
                        type="number"
                        min={10}
                        max={200}
                        value={numQueries}
                        onChange={(e) => setNumQueries(Math.max(10, Math.min(200, parseInt(e.target.value) || 50)))}
                        className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-1 focus:ring-databricks-blue"
                      />
                      <p className="text-xs text-gray-500 mt-1">Total queries across all source tables (10-200)</p>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-databricks-gray-700 mb-1">Query Style</label>
                      <select
                        value={queryStyle}
                        onChange={(e) => setQueryStyle(e.target.value)}
                        className="w-full px-3 py-2 text-sm border border-gray-300 rounded-lg bg-white focus:outline-none focus:ring-1 focus:ring-databricks-blue"
                      >
                        <option value="keyword">Keyword (short 2-5 word queries)</option>
                        <option value="natural">Natural Language (full questions)</option>
                        <option value="mixed">Mixed (alternating keyword and natural)</option>
                      </select>
                      <p className="text-xs text-gray-500 mt-1">Keyword queries test exact matching; natural queries test semantic understanding</p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          <Input
            label="Top K"
            type="number"
            value={topK.toString()}
            onChange={(e) => setTopK(parseInt(e.target.value) || 10)}
            placeholder="10"
            helperText="Number of top results to retrieve"
          />

          {/* Evaluation Settings */}
          <div className="border-t pt-4 mt-4">
            <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
              Evaluation Settings
            </h3>

            <div className="flex items-center space-x-2 mb-4">
              <input
                type="checkbox"
                id="compareQueryTypes"
                checked={compareQueryTypes}
                onChange={(e) => setCompareQueryTypes(e.target.checked)}
                className="w-4 h-4 text-databricks-blue border-gray-300 rounded focus:ring-databricks-blue"
              />
              <label htmlFor="compareQueryTypes" className="text-sm font-medium text-databricks-gray-900">
                Compare query types (FULL_TEXT, ANN, HYBRID)
              </label>
            </div>
            <p className="text-xs text-databricks-gray-600 -mt-2 ml-6 mb-4">
              Compare different search modes on the same index (if supported by your index setup).
            </p>
          </div>

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-md">
                <p className="text-sm text-red-800">{error}</p>
              </div>
            )}

            <Button
              variant="primary"
              onClick={handleSubmit}
              isLoading={isSubmitting}
              disabled={
                !selectedProjectId ||
                !selectedRunId ||
                isSubmitting ||
                (evaluationMode === 'existing' && !existingGoldenTable)
              }
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
          How to Use Evaluation
        </h3>
        <div className="space-y-4 text-sm text-databricks-gray-700">
          <div>
            <p className="font-semibold mb-2">Evaluation Options:</p>
            <ul className="space-y-2 ml-4">
              <li className="flex items-start">
                <span className="mr-2">1.</span>
                <span>
                  <strong>Use Existing Golden Dataset:</strong> Provide a Delta table containing
                  <code className="bg-gray-100 px-1 rounded ml-1">query_text</code> and
                  <code className="bg-gray-100 px-1 rounded ml-1">expected_chunks</code> (chunk text only).
                </span>
              </li>
              <li className="flex items-start">
                <span className="mr-2">2.</span>
                <span>
                  <strong>Auto-Generate Queries:</strong> Generate queries from your build's baseline table and automatically create a golden dataset. The corpus table is auto-detected from your build configuration.
                </span>
              </li>
            </ul>
          </div>

          <div>
            <p className="font-semibold mb-2">Advanced Features:</p>
            <ul className="space-y-2 ml-4">
              <li className="flex items-start">
                <span className="mr-2">•</span>
                <span>
                  <strong>Query Type Comparison:</strong> Test FULL_TEXT (keyword), ANN (semantic), and HYBRID search on the same queries to find the best method.
                </span>
              </li>
            </ul>
          </div>

          <div>
            <p className="font-semibold mb-2">Metrics & Results:</p>
            <ul className="space-y-2 ml-4">
              <li className="flex items-start">
                <span className="mr-2">•</span>
                <span>Evaluation computes Precision@K, Recall@K, and NDCG@K</span>
              </li>
              <li className="flex items-start">
                <span className="mr-2">•</span>
                <span>A retrieved chunk is considered correct if it contains the expected chunk text</span>
              </li>
              <li className="flex items-start">
                <span className="mr-2">•</span>
                <span>Results are logged to MLflow experiments and saved to Delta tables</span>
              </li>
              <li className="flex items-start">
                <span className="mr-2">•</span>
                <span>View detailed analytics including score distributions and top/bottom queries in Databricks job output</span>
              </li>
            </ul>
          </div>
        </div>
      </Card>

      {/* Job Status Block - Below the form */}
      {console.log('[DEBUG RENDER] Checking status card condition:', { jobStatus, submittedRunId, willRender: !!(jobStatus && submittedRunId) })}

      {jobStatus && submittedRunId && (
        <div ref={statusCardRef}>
          <Card className="mt-6 border-4 border-blue-500 shadow-xl bg-blue-50">
            <CardContent className="p-6">
              <div className="flex items-start justify-between mb-4">
                <div>
                  <h2 className="text-lg font-semibold text-databricks-gray-900 mb-2">
                    🎯 Evaluation Job Status
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
                    Evaluation job completed successfully! Redirecting to Review page...
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
        </>
      )}
    </div>
  )
}
