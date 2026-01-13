import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw, ChevronDown, ChevronRight, Clock, CheckCircle, XCircle, PlayCircle } from 'lucide-react'
import { projectsApi } from '../services/projects'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'

type MLflowRun = {
  run_id: string
  run_name: string
  status: string
  start_time: number
  end_time: number | null
  role: string
  metrics: Record<string, number>
  params: Record<string, string>
  tags: Record<string, string>
}

import { evaluationsApi } from '../services/evaluations'

// ... existing imports ...

export default function Review() {
  const { selectedProject, selectedProjectId } = useProject()
  const [mlflowRuns, setMlflowRuns] = useState<MLflowRun[]>([])
  const [selectedRun, setSelectedRun] = useState<MLflowRun | null>(null)
  const [runResults, setRunResults] = useState<any[]>([])
  const [isResultsLoading, setIsResultsLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [mlflowUrl, setMlflowUrl] = useState<string | null>(null)
  const [experimentName, setExperimentName] = useState<string | null>(null)
  const [expandedSections, setExpandedSections] = useState<{
    metrics: boolean
    params: boolean
    tags: boolean
  }>({
    metrics: true,
    params: true,
    tags: false,
  })

  useEffect(() => {
    if (selectedProjectId) {
      loadMLflowData()
    }
  }, [selectedProjectId])

  useEffect(() => {
    if (selectedRun) {
      loadRunResults()
    } else {
      setRunResults([])
    }
  }, [selectedRun])

  const loadRunResults = async () => {
    if (!selectedRun) return

    // Only attempt to load results for runs that might have them
    // (build_strategy, eval_strategy, eval_parent)
    if (!['build_strategy', 'eval_strategy', 'eval_parent'].includes(selectedRun.role)) {
      setRunResults([])
      return
    }

    setIsResultsLoading(true)
    try {
      const results = await evaluationsApi.getResults(selectedRun.run_id)
      setRunResults(results)
    } catch (error) {
      console.error('Failed to load run results:', error)
      setRunResults([])
    } finally {
      setIsResultsLoading(false)
    }
  }

  const loadMLflowData = async () => {
    if (!selectedProjectId) return

    setIsLoading(true)
    try {
      // Load both MLflow URL and runs
      const [mlflowData, runsData] = await Promise.all([
        projectsApi.getMLflowExperiment(selectedProjectId),
        projectsApi.getMLflowRuns(selectedProjectId)
      ])

      setMlflowUrl(mlflowData.mlflow_url)
      setExperimentName(mlflowData.experiment_name)
      setMlflowRuns(runsData.runs)

      // Auto-select first run if available
      if (runsData.runs.length > 0) {
        setSelectedRun(runsData.runs[0])
      }
    } catch (error) {
      console.error('Failed to load MLflow data:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const getStatusBadge = (status: string) => {
    const statusMap: Record<string, { variant: 'success' | 'warning' | 'error' | 'info', icon: any }> = {
      FINISHED: { variant: 'success', icon: CheckCircle },
      RUNNING: { variant: 'info', icon: PlayCircle },
      FAILED: { variant: 'error', icon: XCircle },
      SCHEDULED: { variant: 'warning', icon: Clock },
    }

    const config = statusMap[status] || { variant: 'warning' as const, icon: Clock }
    const Icon = config.icon

    return (
      <Badge variant={config.variant}>
        <Icon className="w-3 h-3 mr-1" />
        {status}
      </Badge>
    )
  }

  const getRoleBadge = (role: string) => {
    const colorMap: Record<string, string> = {
      build_parent: 'bg-blue-100 text-blue-800',
      build_strategy: 'bg-green-100 text-green-800',
      eval_parent: 'bg-purple-100 text-purple-800',
      eval_strategy: 'bg-pink-100 text-pink-800',
    }

    return (
      <span className={`px-2 py-1 text-xs font-medium rounded ${colorMap[role] || 'bg-gray-100 text-gray-800'}`}>
        {role}
      </span>
    )
  }

  const formatDuration = (startTime: number, endTime: number | null) => {
    if (!endTime) return 'Running...'
    const durationMs = endTime - startTime
    const seconds = Math.floor(durationMs / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ${seconds % 60}s`
    const hours = Math.floor(minutes / 60)
    return `${hours}h ${minutes % 60}m`
  }

  const formatTimestamp = (timestamp: number) => {
    return new Date(timestamp).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const formatMetricValue = (value: number) => {
    if (value === Math.floor(value)) return value.toString()
    if (value < 0.001) return value.toExponential(3)
    if (value < 1) return value.toFixed(4)
    return value.toFixed(3)
  }

  const toggleSection = (section: 'metrics' | 'params' | 'tags') => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }))
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-databricks-gray-900">MLflow Experiment Runs</h1>
          <p className="text-sm text-databricks-gray-600 mt-1">
            View all runs, metrics, parameters, and tags for this project
          </p>
        </div>
        <Button
          variant="outline"
          onClick={loadMLflowData}
          icon={<RefreshCw className="w-4 h-4" />}
          disabled={isLoading || !selectedProjectId}
        >
          Refresh
        </Button>
      </div>

      {!selectedProjectId && (
        <Card className="mb-6 bg-yellow-50 border-yellow-200">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to view MLflow runs.
          </p>
        </Card>
      )}

      {selectedProject && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
          <p className="text-sm text-blue-900">
            <span className="font-medium">Current project:</span> {selectedProject.project_name}
          </p>
        </div>
      )}

      {mlflowUrl && experimentName && (
        <Card className="mb-6 bg-gradient-to-r from-blue-50 to-purple-50 border-2 border-blue-300">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-md font-semibold text-databricks-gray-900 mb-2">
                📊 MLflow Experiment
              </h3>
              <p className="text-sm text-databricks-gray-700 font-mono mb-2">
                {experimentName}
              </p>
              <a
                href={mlflowUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center text-sm text-databricks-blue hover:underline font-medium"
              >
                <ExternalLink className="w-4 h-4 mr-1" />
                Open in MLflow UI
              </a>
            </div>
          </div>
        </Card>
      )}

      {isLoading ? (
        <div className="flex justify-center items-center py-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-databricks-blue"></div>
        </div>
      ) : mlflowRuns.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <p className="text-sm text-databricks-gray-600 mb-2">
              No MLflow runs found for this project.
            </p>
            <p className="text-xs text-databricks-gray-500">
              Run a build or evaluation job to see runs here.
            </p>
          </div>
        </Card>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Runs List */}
          <Card className="lg:col-span-1">
            <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
              Runs ({mlflowRuns.length})
            </h2>

            <div className="space-y-2 max-h-[800px] overflow-y-auto custom-scrollbar">
              {mlflowRuns.map((run) => (
                <div
                  key={run.run_id}
                  onClick={() => setSelectedRun(run)}
                  className={`p-3 border rounded-md cursor-pointer transition-all ${selectedRun?.run_id === run.run_id
                    ? 'border-databricks-blue bg-blue-50 ring-2 ring-databricks-blue'
                    : 'border-databricks-gray-200 hover:border-databricks-gray-300 hover:bg-databricks-gray-50'
                    }`}
                >
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-databricks-gray-900 truncate">
                        {run.run_name}
                      </p>
                      <code className="text-xs text-databricks-gray-600 font-mono">
                        {run.run_id.substring(0, 8)}...
                      </code>
                    </div>
                    {getStatusBadge(run.status)}
                  </div>

                  <div className="flex items-center gap-2 mb-2">
                    {getRoleBadge(run.role)}
                  </div>

                  <div className="flex items-center text-xs text-databricks-gray-600">
                    <Clock className="w-3 h-3 mr-1" />
                    {formatDuration(run.start_time, run.end_time)}
                  </div>

                  <p className="text-xs text-databricks-gray-500 mt-1">
                    {formatTimestamp(run.start_time)}
                  </p>
                </div>
              ))}
            </div>
          </Card>

          {/* Run Details */}
          <Card className="lg:col-span-2">
            {!selectedRun ? (
              <div className="text-center py-12">
                <p className="text-sm text-databricks-gray-600">
                  Select a run to view details
                </p>
              </div>
            ) : (
              <div className="space-y-6">
                {/* Run Header */}
                <div>
                  <div className="flex items-start justify-between mb-4">
                    <div>
                      <h2 className="text-xl font-semibold text-databricks-gray-900 mb-2">
                        {selectedRun.run_name}
                      </h2>
                      <div className="flex items-center gap-3 text-sm text-databricks-gray-600">
                        <code className="bg-databricks-gray-100 px-2 py-1 rounded font-mono">
                          {selectedRun.run_id}
                        </code>
                        {getStatusBadge(selectedRun.status)}
                        {getRoleBadge(selectedRun.role)}
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4 p-4 bg-databricks-gray-50 rounded-lg">
                    <div>
                      <p className="text-xs font-medium text-databricks-gray-500 uppercase mb-1">Started</p>
                      <p className="text-sm text-databricks-gray-900">{formatTimestamp(selectedRun.start_time)}</p>
                    </div>
                    <div>
                      <p className="text-xs font-medium text-databricks-gray-500 uppercase mb-1">Duration</p>
                      <p className="text-sm text-databricks-gray-900">
                        {formatDuration(selectedRun.start_time, selectedRun.end_time)}
                      </p>
                    </div>
                  </div>
                </div>

                {/* Metrics Section */}
                {Object.keys(selectedRun.metrics).length > 0 && (
                  <div className="border border-databricks-gray-200 rounded-lg">
                    <button
                      onClick={() => toggleSection('metrics')}
                      className="w-full px-4 py-3 flex items-center justify-between hover:bg-databricks-gray-50 transition-colors"
                    >
                      <h3 className="text-md font-semibold text-databricks-gray-900">
                        📈 Metrics ({Object.keys(selectedRun.metrics).length})
                      </h3>
                      {expandedSections.metrics ? (
                        <ChevronDown className="w-5 h-5 text-databricks-gray-600" />
                      ) : (
                        <ChevronRight className="w-5 h-5 text-databricks-gray-600" />
                      )}
                    </button>

                    {expandedSections.metrics && (
                      <div className="px-4 pb-4 max-h-[400px] overflow-y-auto">
                        <div className="grid grid-cols-2 gap-3">
                          {Object.entries(selectedRun.metrics)
                            .sort(([a], [b]) => a.localeCompare(b))
                            .map(([key, value]) => (
                              <div key={key} className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                                <p className="text-xs text-databricks-gray-600 mb-1 font-medium">{key}</p>
                                <p className="text-lg font-semibold text-databricks-gray-900">
                                  {formatMetricValue(value)}
                                </p>
                              </div>
                            ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Parameters Section */}
                {Object.keys(selectedRun.params).length > 0 && (
                  <div className="border border-databricks-gray-200 rounded-lg">
                    <button
                      onClick={() => toggleSection('params')}
                      className="w-full px-4 py-3 flex items-center justify-between hover:bg-databricks-gray-50 transition-colors"
                    >
                      <h3 className="text-md font-semibold text-databricks-gray-900">
                        ⚙️ Parameters ({Object.keys(selectedRun.params).length})
                      </h3>
                      {expandedSections.params ? (
                        <ChevronDown className="w-5 h-5 text-databricks-gray-600" />
                      ) : (
                        <ChevronRight className="w-5 h-5 text-databricks-gray-600" />
                      )}
                    </button>

                    {expandedSections.params && (
                      <div className="px-4 pb-4 max-h-[400px] overflow-y-auto">
                        <div className="space-y-2">
                          {Object.entries(selectedRun.params)
                            .sort(([a], [b]) => a.localeCompare(b))
                            .map(([key, value]) => (
                              <div key={key} className="flex items-start py-2 border-b border-databricks-gray-100 last:border-0">
                                <span className="text-sm font-medium text-databricks-gray-700 min-w-[200px]">
                                  {key}
                                </span>
                                <span className="text-sm text-databricks-gray-900 font-mono break-all">
                                  {value}
                                </span>
                              </div>
                            ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Tags Section */}
                {Object.keys(selectedRun.tags).length > 0 && (
                  <div className="border border-databricks-gray-200 rounded-lg">
                    <button
                      onClick={() => toggleSection('tags')}
                      className="w-full px-4 py-3 flex items-center justify-between hover:bg-databricks-gray-50 transition-colors"
                    >
                      <h3 className="text-md font-semibold text-databricks-gray-900">
                        🏷️ Tags ({Object.keys(selectedRun.tags).length})
                      </h3>
                      {expandedSections.tags ? (
                        <ChevronDown className="w-5 h-5 text-databricks-gray-600" />
                      ) : (
                        <ChevronRight className="w-5 h-5 text-databricks-gray-600" />
                      )}
                    </button>

                    {expandedSections.tags && (
                      <div className="px-4 pb-4 max-h-[400px] overflow-y-auto">
                        <div className="space-y-2">
                          {Object.entries(selectedRun.tags)
                            .sort(([a], [b]) => a.localeCompare(b))
                            .map(([key, value]) => (
                              <div key={key} className="flex items-start py-2 border-b border-databricks-gray-100 last:border-0">
                                <span className="text-sm font-medium text-databricks-gray-700 min-w-[200px]">
                                  {key}
                                </span>
                                <span className="text-sm text-databricks-gray-600 break-all">
                                  {value}
                                </span>
                              </div>
                            ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Empty State */}
                {Object.keys(selectedRun.metrics).length === 0 &&
                  Object.keys(selectedRun.params).length === 0 &&
                  Object.keys(selectedRun.tags).length === 0 && (
                    <div className="text-center py-8">
                      <p className="text-sm text-databricks-gray-600">
                        No metrics, parameters, or tags recorded for this run.
                      </p>
                    </div>
                  )}
              </div>
            )}
          </Card>
        </div>
      )}

      {/* Detailed Results Section */}
      {selectedRun && (
        <Card className="col-span-1 lg:col-span-3 mt-6">
          <div className="mb-4 flex justify-between items-center">
            <h3 className="text-lg font-semibold text-databricks-gray-900">
              Detailed Results
            </h3>
            <Button
              variant="outline"
              size="sm"
              disabled={isResultsLoading}
              onClick={loadRunResults}
              icon={<RefreshCw className={`w-3 h-3 ${isResultsLoading ? 'animate-spin' : ''}`} />}
            >
              Refresh Results
            </Button>
          </div>

          {isResultsLoading ? (
            <div className="flex justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
            </div>
          ) : runResults.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Query
                    </th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Metrics
                    </th>
                    <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Latency
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {runResults.map((result: any, idx: number) => {
                    const metrics = typeof result.metrics === 'string'
                      ? JSON.parse(result.metrics)
                      : result.metrics || {};

                    return (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-6 py-4 text-sm text-gray-900 max-w-md">
                          <p className="font-medium mb-1">{result.query_text}</p>
                          <p className="text-xs text-gray-500 truncate">
                            Type: {result.query_type}
                          </p>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-500">
                          <div className="space-y-1">
                            {metrics.recall_at_10 !== undefined && (
                              <div className="flex justify-between w-32">
                                <span className="text-xs">Recall@10:</span>
                                <span className="font-mono font-medium">{formatMetricValue(metrics.recall_at_10)}</span>
                              </div>
                            )}
                            {metrics.ndcg_at_10 !== undefined && (
                              <div className="flex justify-between w-32">
                                <span className="text-xs">NDCG@10:</span>
                                <span className="font-mono font-medium">{formatMetricValue(metrics.ndcg_at_10)}</span>
                              </div>
                            )}
                            {metrics.avg_relevance_at_10 !== undefined && (
                              <div className="flex justify-between w-32">
                                <span className="text-xs">Relevance:</span>
                                <span className="font-mono font-medium">{formatMetricValue(metrics.avg_relevance_at_10)}</span>
                              </div>
                            )}
                          </div>
                        </td>
                        <td className="px-6 py-4 text-sm text-gray-500 whitespace-nowrap">
                          <div className="flex items-center">
                            <Clock className="w-3 h-3 mr-1 text-gray-400" />
                            {metrics.retrieval_latency_ms ? `${Math.round(metrics.retrieval_latency_ms)}ms` : '-'}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-8 bg-gray-50 rounded-lg border border-dashed border-gray-300">
              <p className="text-databricks-gray-600">
                No detailed results found for this run.
              </p>
              <p className="text-xs text-databricks-gray-500 mt-1">
                Results are available for evaluation runs that have completed successfully.
              </p>
            </div>
          )}
        </Card>
      )}

      {/* Info Section */}
      {mlflowRuns.length > 0 && (
        <Card className="mt-6 bg-databricks-gray-50">
          <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
            💡 About MLflow Runs
          </h3>
          <ul className="space-y-2 text-sm text-databricks-gray-700">
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Build runs:</strong> Parent runs create indexes and embed documents. Strategy runs test different chunking approaches.
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Evaluation runs:</strong> Parent runs coordinate evaluation. Strategy runs compute metrics like Recall@K and NDCG@K.
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Metrics:</strong> Track retrieval quality (recall, NDCG, precision) and performance (latency, throughput).
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Parameters:</strong> Capture configuration like chunking strategy, embedding model, top-k values, and data sources.
              </span>
            </li>
          </ul>
        </Card>
      )}
    </div>
  )
}
