import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw, ChevronDown, ChevronRight, Clock, CheckCircle, XCircle, PlayCircle, ChevronUp, Check, X, Download } from 'lucide-react'
import Plot from 'react-plotly.js'
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
  const [selectedMetric, setSelectedMetric] = useState<string>('recall')
  const [selectedK, setSelectedK] = useState<number>(5)
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set())
  const [currentPage, setCurrentPage] = useState(1)
  const [searchQuery, setSearchQuery] = useState('')
  const resultsPerPage = 50

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

  const toggleRow = (idx: number) => {
    setExpandedRows(prev => {
      const newSet = new Set(prev)
      if (newSet.has(idx)) {
        newSet.delete(idx)
      } else {
        newSet.add(idx)
      }
      return newSet
    })
  }

  const getQualityColor = (value: number, metricType: 'recall' | 'ndcg' | 'latency' | 'relevance') => {
    if (metricType === 'latency') {
      if (value < 100) return 'text-green-600 bg-green-50 border-green-200'
      if (value < 500) return 'text-yellow-600 bg-yellow-50 border-yellow-200'
      return 'text-red-600 bg-red-50 border-red-200'
    } else {
      if (value >= 0.8) return 'text-green-600 bg-green-50 border-green-200'
      if (value >= 0.5) return 'text-yellow-600 bg-yellow-50 border-yellow-200'
      return 'text-red-600 bg-red-50 border-red-200'
    }
  }

  const processMetricsData = () => {
    const strategyRuns = mlflowRuns.filter(r => r.role === 'eval_strategy')

    const byStrategy: Record<string, MLflowRun[]> = {}
    strategyRuns.forEach(run => {
      const strategy = run.tags?.strategy || run.params?.strategy || 'unknown'
      if (!byStrategy[strategy]) byStrategy[strategy] = []
      byStrategy[strategy].push(run)
    })

    return Object.entries(byStrategy).map(([strategy, runs]) => ({
      strategy,
      recall_at_5: runs[0]?.metrics?.[`recall_at_5`] || 0,
      recall_at_10: runs[0]?.metrics?.[`recall_at_10`] || 0,
      ndcg_at_5: runs[0]?.metrics?.[`ndcg_at_5`] || 0,
      ndcg_at_10: runs[0]?.metrics?.[`ndcg_at_10`] || 0,
      avg_relevance_at_5: runs[0]?.metrics?.[`avg_relevance_at_5`] || 0,
      avg_relevance_at_10: runs[0]?.metrics?.[`avg_relevance_at_10`] || 0,
      avg_latency_ms: runs[0]?.metrics?.[`avg_latency_ms`] || 0,
    }))
  }

  const exportToCSV = () => {
    const headers = ['Query', 'Strategy', 'Type', 'Recall@5', 'NDCG@5', 'Relevance', 'Latency (ms)']
    const rows = runResults.map((result: any) => {
      const metrics = typeof result.metrics === 'string' ? JSON.parse(result.metrics) : result.metrics || {}
      return [
        `"${result.query_text || ''}"`,
        result.strategy || '',
        result.query_type || '',
        metrics.recall_at_5 || '',
        metrics.ndcg_at_5 || '',
        metrics.avg_relevance_at_5 || '',
        metrics.retrieval_latency_ms || ''
      ].join(',')
    })

    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `evaluation-results-${selectedRun?.run_id || 'export'}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
  }

  const filteredResults = runResults.filter((result: any) => {
    if (!searchQuery) return true
    return result.query_text?.toLowerCase().includes(searchQuery.toLowerCase())
  })

  const paginatedResults = filteredResults.slice(
    (currentPage - 1) * resultsPerPage,
    currentPage * resultsPerPage
  )

  const totalPages = Math.ceil(filteredResults.length / resultsPerPage)

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

      {/* Metrics Visualization */}
      {mlflowRuns.length > 0 && mlflowRuns.some(r => r.role === 'eval_strategy') && (
        <Card className="mb-6">
          <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
            📈 Metrics Comparison
          </h2>

          <div className="flex gap-4 mb-4">
            <div>
              <label className="text-xs font-medium text-databricks-gray-700 mb-1 block">Metric</label>
              <select
                value={selectedMetric}
                onChange={(e) => setSelectedMetric(e.target.value)}
                className="px-3 py-2 border border-databricks-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-blue"
              >
                <option value="recall">Recall</option>
                <option value="ndcg">NDCG</option>
                <option value="avg_relevance">Relevance</option>
                <option value="avg_latency_ms">Latency (ms)</option>
              </select>
            </div>

            <div>
              <label className="text-xs font-medium text-databricks-gray-700 mb-1 block">K Value</label>
              <select
                value={selectedK}
                onChange={(e) => setSelectedK(Number(e.target.value))}
                className="px-3 py-2 border border-databricks-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-blue"
                disabled={selectedMetric === 'avg_latency_ms'}
              >
                <option value={5}>@5</option>
                <option value={10}>@10</option>
              </select>
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Strategy Comparison Bar Chart */}
            <div>
              <Plot
                data={[
                  {
                    x: processMetricsData().map(d => d.strategy),
                    y: processMetricsData().map(d => {
                      const metricKey = selectedMetric === 'avg_latency_ms'
                        ? 'avg_latency_ms'
                        : `${selectedMetric}_at_${selectedK}`
                      return d[metricKey as keyof typeof d] || 0
                    }),
                    type: 'bar',
                    marker: {
                      color: processMetricsData().map((_, idx) =>
                        ['#0066cc', '#00a86b', '#8b4789'][idx % 3]
                      ),
                    },
                    text: processMetricsData().map(d => {
                      const metricKey = selectedMetric === 'avg_latency_ms'
                        ? 'avg_latency_ms'
                        : `${selectedMetric}_at_${selectedK}`
                      const value = d[metricKey as keyof typeof d] || 0
                      return typeof value === 'number' ? value.toFixed(3) : value
                    }),
                    textposition: 'outside',
                  },
                ]}
                layout={{
                  title: `${selectedMetric === 'avg_latency_ms' ? 'Latency' : selectedMetric.toUpperCase()}${selectedMetric !== 'avg_latency_ms' ? `@${selectedK}` : ''} by Strategy`,
                  xaxis: { title: 'Strategy' },
                  yaxis: { title: selectedMetric === 'avg_latency_ms' ? 'Latency (ms)' : 'Score' },
                  height: 350,
                  margin: { t: 50, b: 50, l: 60, r: 20 },
                }}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: '100%' }}
              />
            </div>

            {/* Latency vs Quality Scatter */}
            <div>
              <Plot
                data={[
                  {
                    x: processMetricsData().map(d => d.avg_latency_ms),
                    y: processMetricsData().map(d => d[`recall_at_${selectedK}` as keyof typeof d] || 0),
                    mode: 'markers',
                    type: 'scatter',
                    text: processMetricsData().map(d => d.strategy),
                    marker: {
                      size: 12,
                      color: processMetricsData().map((_, idx) =>
                        ['#0066cc', '#00a86b', '#8b4789'][idx % 3]
                      ),
                    },
                  },
                ]}
                layout={{
                  title: `Latency vs Recall@${selectedK}`,
                  xaxis: { title: 'Latency (ms)' },
                  yaxis: { title: `Recall@${selectedK}` },
                  height: 350,
                  margin: { t: 50, b: 50, l: 60, r: 20 },
                }}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: '100%' }}
              />
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

      {/* Enhanced Query Results Section */}
      {selectedRun && (
        <Card className="col-span-1 lg:col-span-3 mt-6">
          <div className="mb-4 flex justify-between items-center">
            <h3 className="text-lg font-semibold text-databricks-gray-900">
              📋 Query Results
            </h3>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={exportToCSV}
                disabled={runResults.length === 0}
                icon={<Download className="w-3 h-3" />}
              >
                Export CSV
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={isResultsLoading}
                onClick={loadRunResults}
                icon={<RefreshCw className={`w-3 h-3 ${isResultsLoading ? 'animate-spin' : ''}`} />}
              >
                Refresh
              </Button>
            </div>
          </div>

          {/* Search Box */}
          {runResults.length > 0 && (
            <div className="mb-4">
              <input
                type="text"
                placeholder="Search queries..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value)
                  setCurrentPage(1)
                }}
                className="px-4 py-2 border border-databricks-gray-300 rounded-md text-sm w-full max-w-md focus:outline-none focus:ring-2 focus:ring-databricks-blue"
              />
            </div>
          )}

          {isResultsLoading ? (
            <div className="flex justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
            </div>
          ) : paginatedResults.length > 0 ? (
            <>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="w-8"></th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Query
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Strategy
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Type
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Recall@5
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        NDCG@5
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Relevance
                      </th>
                      <th scope="col" className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                        Latency
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {paginatedResults.map((result: any, idx: number) => {
                      const globalIdx = (currentPage - 1) * resultsPerPage + idx
                      const metrics = typeof result.metrics === 'string' ? JSON.parse(result.metrics) : result.metrics || {}
                      const expectedChunks = result.expected_chunks ? JSON.parse(result.expected_chunks) : []
                      const retrievedChunks = result.retrieved_chunks ? JSON.parse(result.retrieved_chunks) : []
                      const isExpanded = expandedRows.has(globalIdx)

                      const recall = metrics.recall_at_5 || 0
                      const ndcg = metrics.ndcg_at_5 || 0
                      const relevance = metrics.avg_relevance_at_5 || 0
                      const latency = metrics.retrieval_latency_ms || 0

                      return (
                        <>
                          <tr key={globalIdx} className="hover:bg-gray-50 cursor-pointer">
                            <td className="px-2 py-3">
                              <button onClick={() => toggleRow(globalIdx)} className="p-1 hover:bg-gray-200 rounded">
                                {isExpanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                              </button>
                            </td>
                            <td className="px-4 py-3 text-sm text-gray-900 max-w-xs">
                              <p className="truncate" title={result.query_text}>
                                {result.query_text}
                              </p>
                            </td>
                            <td className="px-4 py-3">
                              <Badge variant="info">{result.strategy || 'N/A'}</Badge>
                            </td>
                            <td className="px-4 py-3">
                              <Badge variant={result.query_type === 'ANN' ? 'success' : 'warning'}>
                                {result.query_type || 'N/A'}
                              </Badge>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded text-xs font-medium border ${getQualityColor(recall, 'recall')}`}>
                                {formatMetricValue(recall)}
                              </span>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded text-xs font-medium border ${getQualityColor(ndcg, 'ndcg')}`}>
                                {formatMetricValue(ndcg)}
                              </span>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded text-xs font-medium border ${getQualityColor(relevance, 'relevance')}`}>
                                {formatMetricValue(relevance)}
                              </span>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded text-xs font-medium border ${getQualityColor(latency, 'latency')}`}>
                                {Math.round(latency)}ms
                              </span>
                            </td>
                          </tr>

                          {/* Expanded Row Details */}
                          {isExpanded && (
                            <tr key={`${globalIdx}-expanded`}>
                              <td colSpan={8} className="px-6 py-4 bg-gray-50">
                                <div className="space-y-4">
                                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                                    {/* Expected Chunks */}
                                    {expectedChunks.length > 0 && (
                                      <div>
                                        <h4 className="text-sm font-semibold text-gray-900 mb-2">
                                          Expected Chunks (Ground Truth)
                                        </h4>
                                        <div className="border rounded-md overflow-hidden">
                                          <table className="min-w-full text-xs">
                                            <thead className="bg-gray-100">
                                              <tr>
                                                <th className="px-3 py-2 text-left">Chunk ID</th>
                                                <th className="px-3 py-2 text-left">Rank</th>
                                                <th className="px-3 py-2 text-left">Match</th>
                                              </tr>
                                            </thead>
                                            <tbody className="bg-white">
                                              {expectedChunks.slice(0, 10).map((chunkId: string, i: number) => {
                                                const isInRetrieved = retrievedChunks.includes(chunkId)
                                                return (
                                                  <tr key={i} className="border-t">
                                                    <td className="px-3 py-2 font-mono text-xs">{chunkId}</td>
                                                    <td className="px-3 py-2">{i + 1}</td>
                                                    <td className="px-3 py-2">
                                                      {isInRetrieved ? (
                                                        <Check className="w-4 h-4 text-green-600" />
                                                      ) : (
                                                        <X className="w-4 h-4 text-red-600" />
                                                      )}
                                                    </td>
                                                  </tr>
                                                )
                                              })}
                                            </tbody>
                                          </table>
                                        </div>
                                      </div>
                                    )}

                                    {/* Retrieved Chunks */}
                                    {retrievedChunks.length > 0 && (
                                      <div>
                                        <h4 className="text-sm font-semibold text-gray-900 mb-2">
                                          Retrieved Chunks (Actual Results)
                                        </h4>
                                        <div className="border rounded-md overflow-hidden">
                                          <table className="min-w-full text-xs">
                                            <thead className="bg-gray-100">
                                              <tr>
                                                <th className="px-3 py-2 text-left">Chunk ID</th>
                                                <th className="px-3 py-2 text-left">Rank</th>
                                                <th className="px-3 py-2 text-left">Match</th>
                                              </tr>
                                            </thead>
                                            <tbody className="bg-white">
                                              {retrievedChunks.slice(0, 10).map((chunkId: string, i: number) => {
                                                const isInExpected = expectedChunks.includes(chunkId)
                                                return (
                                                  <tr key={i} className="border-t">
                                                    <td className="px-3 py-2 font-mono text-xs">{chunkId}</td>
                                                    <td className="px-3 py-2">{i + 1}</td>
                                                    <td className="px-3 py-2">
                                                      {isInExpected ? (
                                                        <Check className="w-4 h-4 text-green-600" />
                                                      ) : (
                                                        <X className="w-4 h-4 text-gray-400" />
                                                      )}
                                                    </td>
                                                  </tr>
                                                )
                                              })}
                                            </tbody>
                                          </table>
                                        </div>
                                      </div>
                                    )}
                                  </div>

                                  {/* Metrics Breakdown */}
                                  <div>
                                    <h4 className="text-sm font-semibold text-gray-900 mb-2">All Metrics</h4>
                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                                      {Object.entries(metrics).map(([key, value]: [string, any]) => (
                                        <div key={key} className="p-2 bg-white border rounded-md">
                                          <p className="text-xs text-gray-600">{key}</p>
                                          <p className="text-sm font-semibold text-gray-900">
                                            {typeof value === 'number' ? formatMetricValue(value) : value}
                                          </p>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          )}
                        </>
                      )
                    })}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="mt-4 flex items-center justify-between">
                  <p className="text-sm text-gray-600">
                    Showing {(currentPage - 1) * resultsPerPage + 1} to{' '}
                    {Math.min(currentPage * resultsPerPage, filteredResults.length)} of{' '}
                    {filteredResults.length} results
                  </p>
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                      disabled={currentPage === 1}
                    >
                      Previous
                    </Button>
                    <span className="px-3 py-1 text-sm">
                      Page {currentPage} of {totalPages}
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                      disabled={currentPage === totalPages}
                    >
                      Next
                    </Button>
                  </div>
                </div>
              )}
            </>
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
