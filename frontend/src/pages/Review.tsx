import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw, ChevronDown, ChevronRight } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { evaluationsApi } from '../services/evaluations'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '../components/ui/Table'
import { BuildJob } from '../types'

export default function Review() {
  const { selectedProject, selectedProjectId } = useProject()
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [selectedBuild, setSelectedBuild] = useState<BuildJob | null>(null)
  const [evalResults, setEvalResults] = useState<any[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isLoadingResults, setIsLoadingResults] = useState(false)
  const [expandedQuery, setExpandedQuery] = useState<string | null>(null)

  useEffect(() => {
    if (selectedProjectId) {
      loadBuilds()
    }
  }, [selectedProjectId])

  const loadBuilds = async () => {
    setIsLoading(true)
    try {
      const buildsData = await buildsApi.list()
      const projectBuilds = buildsData.filter(
        (b: BuildJob) => b.project_id === selectedProjectId
      )
      setBuilds(projectBuilds)
    } catch (error) {
      console.error('Failed to load builds:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const loadEvalResults = async (build: BuildJob) => {
    setIsLoadingResults(true)
    setSelectedBuild(build)
    try {
      const results = await evaluationsApi.getResults(build.run_id)
      setEvalResults(results)
    } catch (error) {
      console.error('Failed to load evaluation results:', error)
      setEvalResults([])
    } finally {
      setIsLoadingResults(false)
    }
  }

  const getStateBadge = (state: string) => {
    const stateMap: Record<string, 'success' | 'warning' | 'error' | 'info'> = {
      SUCCESS: 'success',
      RUNNING: 'info',
      PENDING: 'warning',
      FAILED: 'error',
    }
    return <Badge variant={stateMap[state] || 'default'}>{state}</Badge>
  }

  const formatMetric = (value: number | null | undefined, decimals = 3) => {
    if (value === null || value === undefined) return '-'
    return value.toFixed(decimals)
  }

  // Group results by strategy
  const resultsByStrategy = evalResults.reduce((acc: any, result: any) => {
    const strategy = result.strategy || 'unknown'
    if (!acc[strategy]) {
      acc[strategy] = []
    }
    acc[strategy].push(result)
    return acc
  }, {})

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-databricks-gray-900">Review Results</h1>
          <p className="text-sm text-databricks-gray-600 mt-1">
            View build and evaluation job results with detailed metrics
          </p>
        </div>
        <Button
          variant="outline"
          onClick={loadBuilds}
          icon={<RefreshCw className="w-4 h-4" />}
          disabled={isLoading || !selectedProjectId}
        >
          Refresh
        </Button>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to view results.
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

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Build Runs List */}
        <Card className="lg:col-span-1">
          <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">Build Runs</h2>
          
          {isLoading ? (
            <div className="flex justify-center items-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
            </div>
          ) : builds.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-sm text-databricks-gray-600">
                No build runs yet. Create a build first.
              </p>
            </div>
          ) : (
            <div className="space-y-2 max-h-[600px] overflow-y-auto custom-scrollbar">
              {builds.map((build) => (
                <div
                  key={build.run_id}
                  onClick={() => loadEvalResults(build)}
                  className={`p-3 border rounded-md cursor-pointer transition-all ${
                    selectedBuild?.run_id === build.run_id
                      ? 'border-databricks-blue bg-blue-50'
                      : 'border-databricks-gray-200 hover:border-databricks-gray-300 hover:bg-databricks-gray-50'
                  }`}
                >
                  <div className="flex items-start justify-between mb-2">
                    <code className="text-xs font-mono bg-white px-2 py-1 rounded border border-databricks-gray-200">
                      {build.run_id.substring(0, 12)}...
                    </code>
                    {getStateBadge(build.state)}
                  </div>
                  <p className="text-xs text-databricks-gray-600">
                    {new Date(build.created_at).toLocaleString()}
                  </p>
                  {build.config?.data_type && (
                    <p className="text-xs text-databricks-gray-500 mt-1">
                      {build.config.data_type}
                    </p>
                  )}
                </div>
              ))}
            </div>
          )}
        </Card>

        {/* Evaluation Results */}
        <Card className="lg:col-span-2">
          <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
            Evaluation Results
            {selectedBuild && (
              <span className="text-sm font-normal text-databricks-gray-600 ml-2">
                for {selectedBuild.run_id.substring(0, 12)}...
              </span>
            )}
          </h2>

          {!selectedBuild ? (
            <div className="text-center py-12">
              <p className="text-sm text-databricks-gray-600">
                Select a build run to view evaluation results
              </p>
            </div>
          ) : isLoadingResults ? (
            <div className="flex justify-center items-center py-12">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
            </div>
          ) : evalResults.length === 0 ? (
            <div className="text-center py-12">
              <p className="text-sm text-databricks-gray-600 mb-4">
                No evaluation results found for this build.
              </p>
              <p className="text-xs text-databricks-gray-500">
                Run an evaluation job on this build to see metrics here.
              </p>
            </div>
          ) : (
            <div className="space-y-6">
              {Object.entries(resultsByStrategy).map(([strategy, results]: [string, any]) => {
                const strategyResults = results as any[]
                const avgMetrics = {
                  recall_at_10: strategyResults.reduce((sum, r) => {
                    const m = typeof r.metrics === 'string' ? JSON.parse(r.metrics) : r.metrics
                    return sum + (m.recall_at_10 || 0)
                  }, 0) / strategyResults.length,
                  ndcg_at_10: strategyResults.reduce((sum, r) => {
                    const m = typeof r.metrics === 'string' ? JSON.parse(r.metrics) : r.metrics
                    return sum + (m.ndcg_at_10 || 0)
                  }, 0) / strategyResults.length,
                  latency_ms: strategyResults.reduce((sum, r) => {
                    const m = typeof r.metrics === 'string' ? JSON.parse(r.metrics) : r.metrics
                    return sum + (m.retrieval_latency_ms || 0)
                  }, 0) / strategyResults.length,
                }

                return (
                  <div key={strategy} className="border border-databricks-gray-200 rounded-lg p-4">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="text-md font-semibold text-databricks-gray-900">
                        {strategy}
                      </h3>
                      <Badge variant="default">{strategyResults.length} queries</Badge>
                    </div>

                    <div className="grid grid-cols-3 gap-4 mb-4">
                      <div className="bg-databricks-gray-50 p-3 rounded">
                        <p className="text-xs text-databricks-gray-600 mb-1">Avg Recall@10</p>
                        <p className="text-lg font-semibold text-databricks-gray-900">
                          {formatMetric(avgMetrics.recall_at_10)}
                        </p>
                      </div>
                      <div className="bg-databricks-gray-50 p-3 rounded">
                        <p className="text-xs text-databricks-gray-600 mb-1">Avg NDCG@10</p>
                        <p className="text-lg font-semibold text-databricks-gray-900">
                          {formatMetric(avgMetrics.ndcg_at_10)}
                        </p>
                      </div>
                      <div className="bg-databricks-gray-50 p-3 rounded">
                        <p className="text-xs text-databricks-gray-600 mb-1">Avg Latency</p>
                        <p className="text-lg font-semibold text-databricks-gray-900">
                          {formatMetric(avgMetrics.latency_ms, 0)}ms
                        </p>
                      </div>
                    </div>

                    <div className="border-t border-databricks-gray-200 pt-3">
                      <button
                        onClick={() => setExpandedQuery(expandedQuery === strategy ? null : strategy)}
                        className="flex items-center text-sm text-databricks-blue hover:underline"
                      >
                        {expandedQuery === strategy ? (
                          <ChevronDown className="w-4 h-4 mr-1" />
                        ) : (
                          <ChevronRight className="w-4 h-4 mr-1" />
                        )}
                        View per-query metrics
                      </button>

                      {expandedQuery === strategy && (
                        <div className="mt-3 max-h-[300px] overflow-y-auto custom-scrollbar">
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Query</TableHead>
                                <TableHead>Recall@10</TableHead>
                                <TableHead>NDCG@10</TableHead>
                                <TableHead>Latency (ms)</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {strategyResults.map((result, idx) => {
                                const metrics = typeof result.metrics === 'string' 
                                  ? JSON.parse(result.metrics) 
                                  : result.metrics
                                return (
                                  <TableRow key={idx}>
                                    <TableCell className="max-w-xs truncate">
                                      {result.query_text || '-'}
                                    </TableCell>
                                    <TableCell>{formatMetric(metrics.recall_at_10)}</TableCell>
                                    <TableCell>{formatMetric(metrics.ndcg_at_10)}</TableCell>
                                    <TableCell>{formatMetric(metrics.retrieval_latency_ms, 0)}</TableCell>
                                  </TableRow>
                                )
                              })}
                            </TableBody>
                          </Table>
                        </div>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </Card>
      </div>

      {builds.length > 0 && (
        <Card className="mt-6 bg-databricks-gray-50">
          <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
            About Results
          </h3>
          <ul className="space-y-2 text-sm text-databricks-gray-700">
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                Results are fetched from the eval_results table in your Delta Lake catalog
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                Metrics shown: Recall@10, NDCG@10, and retrieval latency per query
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                All evaluation runs are tracked in MLflow - visit the MLflow UI in Databricks for detailed logs
              </span>
            </li>
          </ul>
        </Card>
      )}
    </div>
  )
}
