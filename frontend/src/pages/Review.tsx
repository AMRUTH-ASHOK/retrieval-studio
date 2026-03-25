import { useState, useEffect, useMemo } from 'react'
import { ExternalLink, RefreshCw, Search, ChevronLeft, ChevronRight, ChevronDown, ChevronUp, Filter } from 'lucide-react'
import { useProject } from '../context/ProjectContext'
import { buildsApi } from '../services/builds'
import { projectsApi } from '../services/projects'
import { BuildJob, Evaluation } from '../types'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import BuildSelector from '../components/review/BuildSelector'
import EvaluationSelector from '../components/review/EvaluationSelector'
import BestPerformers from '../components/review/BestPerformers'
import MetricsBarCharts from '../components/review/MetricsBarCharts'
import ComparisonTable from '../components/review/ComparisonTable'
import SourceComparison from '../components/review/SourceComparison'
import type { MLflowRun } from '../utils/metricsAggregation'
import { evaluationsApi } from '../services/evaluations'
import api from '../services/api'

export default function Review() {
  const { selectedProject, selectedProjectId } = useProject()

  // Data loading states
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [evaluationsByBuild, setEvaluationsByBuild] = useState<Map<string, Evaluation[]>>(new Map())
  const [mlflowRuns, setMlflowRuns] = useState<MLflowRun[]>([])
  const [mlflowUrl, setMlflowUrl] = useState<string | null>(null)
  const [experimentName, setExperimentName] = useState<string | null>(null)

  // Selection states
  const [selectedBuildIds, setSelectedBuildIds] = useState<Set<string>>(new Set())
  const [selectedEvalIds, setSelectedEvalIds] = useState<Set<string>>(new Set())
  const [reviewScope, setReviewScope] = useState<'project' | 'build' | 'evaluation'>('evaluation')

  // UI states
  const [isLoadingBuilds, setIsLoadingBuilds] = useState(false)
  const [isLoadingEvals, setIsLoadingEvals] = useState(false)
  const [isLoadingMetrics, setIsLoadingMetrics] = useState(false)
  const [showResults, setShowResults] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Aggregated metrics states
  const [buildMetrics, setBuildMetrics] = useState<any[]>([])
  const [strategyMetrics, setStrategyMetrics] = useState<any[]>([])
  const [evaluationMetrics, setEvaluationMetrics] = useState<any[]>([])
  const [bestPerformers, setBestPerformers] = useState({
    bestBuild: null,
    bestStrategy: null,
    fastest: null,
    bestOverall: null
  })
  const [queryDetails, setQueryDetails] = useState<any[]>([])
  const [selectedDetailsEvalId, setSelectedDetailsEvalId] = useState<string | null>(null)
  const [isLoadingDetails, setIsLoadingDetails] = useState(false)
  const [indexSelections, setIndexSelections] = useState<any[]>([])

  const loadIndexSelections = async () => {
    if (!selectedProjectId) return
    try {
      const response = await api.get(`/cleanup/projects/${selectedProjectId}/indexes`)
      setIndexSelections(response.data || [])
    } catch { setIndexSelections([]) }
  }

  const handleUpdateIndexStatus = async (updates: { id: string; status: string }[]) => {
    if (!selectedProjectId) return
    try {
      await api.put(`/cleanup/projects/${selectedProjectId}/indexes/status`, { updates })
      loadIndexSelections()
    } catch (e) { console.error('Failed to update index status:', e) }
  }

  const handleRequestExplanation = async (sourceName: string, strategies: any[]): Promise<string> => {
    const response = await api.post('/evaluations/explain', {
      source_name: sourceName,
      strategies: strategies.map(s => ({
        strategy_name: s.strategyName || s.strategy_name,
        recall_at_10: s.metrics?.recall_at_10 || 0,
        ndcg_at_10: s.metrics?.ndcg_at_10 || 0,
        precision_at_10: s.metrics?.precision_at_10 || 0,
        avg_latency_ms: s.metrics?.avg_latency_ms || 0,
      }))
    })
    return response.data.explanation
  }

  // Load builds when project changes
  useEffect(() => {
    if (selectedProjectId) {
      loadBuilds()
      loadMLflowExperiment()
    } else {
      setBuilds([])
      setSelectedBuildIds(new Set())
      setSelectedEvalIds(new Set())
      setEvaluationsByBuild(new Map())
    }
  }, [selectedProjectId])

  useEffect(() => {
    setShowResults(false)
    setError(null)
    setQueryDetails([])
    setSelectedDetailsEvalId(null)
    if (reviewScope !== 'evaluation') {
      setSelectedEvalIds(new Set())
      setEvaluationsByBuild(new Map())
    }
  }, [reviewScope])

  // Load evaluations when selected builds change
  useEffect(() => {
    if (reviewScope === 'evaluation' && selectedBuildIds.size > 0) {
      loadEvaluationsForSelectedBuilds()
    } else {
      setEvaluationsByBuild(new Map())
      setSelectedEvalIds(new Set())
      setShowResults(false)
    }
  }, [selectedBuildIds, reviewScope])

  useEffect(() => {
    if (reviewScope === 'evaluation' && selectedEvalIds.size > 0 && !selectedDetailsEvalId) {
      const first = Array.from(selectedEvalIds)[0]
      setSelectedDetailsEvalId(first)
    }
  }, [reviewScope, selectedEvalIds, selectedDetailsEvalId])

  const loadBuilds = async () => {
    if (!selectedProjectId) return

    setIsLoadingBuilds(true)
    try {
      const buildsData = await buildsApi.getByProject(selectedProjectId)
      setBuilds(buildsData)
    } catch (error) {
      console.error('Failed to load builds:', error)
    } finally {
      setIsLoadingBuilds(false)
    }
  }

  const loadMLflowExperiment = async () => {
    if (!selectedProjectId) return

    try {
      const mlflowData = await projectsApi.getMLflowExperiment(selectedProjectId)
      setMlflowUrl(mlflowData.mlflow_url)
      setExperimentName(mlflowData.experiment_name)
    } catch (error) {
      console.error('Failed to load MLflow experiment:', error)
    }
  }

  const loadEvaluationsForSelectedBuilds = async () => {
    setIsLoadingEvals(true)
    try {
      const newEvalsByBuild = new Map<string, Evaluation[]>()

      // Fetch evaluations for each selected build
      await Promise.all(
        Array.from(selectedBuildIds).map(async (buildId) => {
          try {
            const evals = await evaluationsApi.getByBuildRun(buildId)
            newEvalsByBuild.set(buildId, evals)
          } catch (error) {
            console.error(`Failed to load evaluations for build ${buildId}:`, error)
            newEvalsByBuild.set(buildId, [])
          }
        })
      )

      setEvaluationsByBuild(newEvalsByBuild)
    } catch (error) {
      console.error('Failed to load evaluations:', error)
    } finally {
      setIsLoadingEvals(false)
    }
  }

  const loadMetricsForScope = async () => {
    if (!selectedProjectId) return

    if (reviewScope === 'build' && selectedBuildIds.size === 0) {
      setError('Select at least one build to review.')
      setShowResults(false)
      return
    }

    if (reviewScope === 'evaluation' && selectedEvalIds.size === 0) {
      setError('Select at least one evaluation to review.')
      setShowResults(false)
      return
    }

    setIsLoadingMetrics(true)
    try {
      const runsData = await projectsApi.getMLflowRuns(selectedProjectId)

      if (!runsData || !Array.isArray(runsData.runs)) {
        console.error('Invalid MLflow runs response:', runsData)
        setMlflowRuns([])
        setShowResults(false)
        return
      }

      const evalRuns = runsData.runs.filter((run: MLflowRun) => run && run.role === 'eval_strategy')
      let filteredRuns = evalRuns

      if (reviewScope === 'build') {
        filteredRuns = evalRuns.filter(run => {
          const buildRunId = run.params?.build_run_id || run.tags?.build_run_id
          return buildRunId ? selectedBuildIds.has(buildRunId) : false
        })
      } else if (reviewScope === 'evaluation') {
        filteredRuns = evalRuns.filter(run => {
          const evalId = run.params?.eval_id || run.tags?.rs_eval_id || run.tags?.eval_id
          return evalId ? selectedEvalIds.has(evalId) : false
        })
      }

      setMlflowRuns(filteredRuns)
      setShowResults(true)
      setError(null)
      loadIndexSelections()
    } catch (error: any) {
      console.error('Failed to load metrics:', error)
      setMlflowRuns([])
      setShowResults(false)
      setError(error?.message || 'Failed to load metrics. Please check the console for details.')
    } finally {
      setIsLoadingMetrics(false)
    }
  }

  const loadQueryDetails = async (evalId: string) => {
    setIsLoadingDetails(true)
    setError(null)
    try {
      const results = await evaluationsApi.getResults(evalId)

      if (!results || results.length === 0) {
        setQueryDetails([])
        setSelectedDetailsEvalId(evalId)
        setError('No query results found. The evaluation may still be running, or the results table may not have been created yet. Check the job status to ensure it completed successfully.')
        return
      }

      const parsed = results.map((row: any) => {
        const parseJson = (value: any) => {
          if (!value) return null
          if (typeof value === 'object') return value
          try {
            return JSON.parse(value)
          } catch {
            return value
          }
        }

        return {
          ...row,
          expected_chunks_parsed: parseJson(row.expected_chunks),
          retrieved_chunks_parsed: parseJson(row.retrieved_chunks),
          metrics_parsed: parseJson(row.metrics),
        }
      })

      setQueryDetails(parsed)
      setSelectedDetailsEvalId(evalId)
    } catch (error: any) {
      console.error('Failed to load query details:', error)
      setQueryDetails([])
      setError(error?.response?.data?.detail || 'Failed to load query details. Please check if the evaluation completed successfully.')
    } finally {
      setIsLoadingDetails(false)
    }
  }

  const handleToggleBuild = (buildId: string) => {
    setSelectedBuildIds(prev => {
      const newSet = new Set(prev)
      if (newSet.has(buildId)) {
        newSet.delete(buildId)

        // Also deselect any evaluations from this build
        const evals = evaluationsByBuild.get(buildId) || []
        const evalIds = new Set(evals.map(e => e.run_id))
        setSelectedEvalIds(prevEvals => {
          const newEvalSet = new Set(prevEvals)
          evalIds.forEach(id => newEvalSet.delete(id))
          return newEvalSet
        })
      } else {
        newSet.add(buildId)
      }
      return newSet
    })
    setShowResults(false)
  }

  const handleToggleEvaluation = (evalId: string) => {
    setSelectedEvalIds(prev => {
      const newSet = new Set(prev)
      if (newSet.has(evalId)) {
        newSet.delete(evalId)
      } else {
        newSet.add(evalId)
      }
      return newSet
    })
    setShowResults(false)
  }

  const handleReviewClick = () => {
    loadMetricsForScope()
  }

  const handleRefresh = () => {
    loadBuilds()
    loadMLflowExperiment()
    if (reviewScope === 'evaluation' && selectedBuildIds.size > 0) {
      loadEvaluationsForSelectedBuilds()
    }
    if (reviewScope === 'project') {
      loadMetricsForScope()
    } else if (reviewScope === 'build' && selectedBuildIds.size > 0) {
      loadMetricsForScope()
    } else if (reviewScope === 'evaluation' && selectedEvalIds.size > 0) {
      loadMetricsForScope()
    }
  }

  const [querySearch, setQuerySearch] = useState('')
  const [queryPage, setQueryPage] = useState(0)
  const [expandedQueries, setExpandedQueries] = useState<Set<number>>(new Set())
  const [expandedChunks, setExpandedChunks] = useState<Set<string>>(new Set())
  const QUERIES_PER_PAGE = 20

  const filteredQueryDetails = useMemo(() => {
    if (!querySearch.trim()) return queryDetails
    const term = querySearch.toLowerCase()
    return queryDetails.filter((row: any) =>
      (row.query_text || '').toLowerCase().includes(term)
    )
  }, [queryDetails, querySearch])

  const pagedQueryDetails = useMemo(() => {
    const start = queryPage * QUERIES_PER_PAGE
    return filteredQueryDetails.slice(start, start + QUERIES_PER_PAGE)
  }, [filteredQueryDetails, queryPage])

  const totalQueryPages = Math.ceil(filteredQueryDetails.length / QUERIES_PER_PAGE)

  const toggleQuery = (idx: number) => {
    setExpandedQueries(prev => {
      const n = new Set(prev)
      n.has(idx) ? n.delete(idx) : n.add(idx)
      return n
    })
  }

  const toggleChunk = (key: string) => {
    setExpandedChunks(prev => {
      const n = new Set(prev)
      n.has(key) ? n.delete(key) : n.add(key)
      return n
    })
  }

  const renderChunkCard = (chunk: any, idx: number, label: string) => {
    if (typeof chunk === 'string') {
      return (
        <div key={`${label}-${idx}`} className="p-2.5 bg-gray-50 rounded-lg border border-gray-200 text-xs font-mono text-gray-700">
          {chunk}
        </div>
      )
    }

    const chunkId = chunk.chunk_id || chunk.id || `chunk_${idx}`
    const chunkText = chunk.chunk_text || chunk.text || ''
    const score = chunk.score
    const expandKey = `${label}-${chunkId}-${idx}`
    const isExpanded = expandedChunks.has(expandKey)
    const needsTruncation = chunkText.length > 300

    return (
      <div key={expandKey} className="p-3 bg-gray-50 rounded-lg border border-gray-200">
        <div className="flex items-start justify-between gap-2">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-mono text-gray-400 truncate">{chunkId}</span>
              {score !== undefined && (
                <span className={`text-[10px] font-medium px-1.5 py-0.5 rounded-full ${
                  score >= 0.8 ? 'bg-green-100 text-green-700'
                  : score >= 0.5 ? 'bg-yellow-100 text-yellow-700'
                  : 'bg-red-100 text-red-700'
                }`}>
                  {typeof score === 'number' ? score.toFixed(3) : score}
                </span>
              )}
            </div>
          </div>
        </div>
        <div className="mt-1.5 text-xs text-gray-700 leading-relaxed whitespace-pre-wrap">
          {isExpanded || !needsTruncation ? chunkText : `${chunkText.slice(0, 300)}...`}
        </div>
        {needsTruncation && (
          <button onClick={() => toggleChunk(expandKey)} className="mt-1 text-[10px] text-databricks-blue hover:underline">
            {isExpanded ? 'Show less' : 'Show full text'}
          </button>
        )}
      </div>
    )
  }

  const renderChunks = (chunks: any[] | null, label: string) => {
    if (!chunks || !Array.isArray(chunks) || chunks.length === 0) {
      return <p className="text-xs text-gray-400 italic">No {label} chunks</p>
    }
    return (
      <div className="space-y-2">
        {chunks.map((chunk, idx) => renderChunkCard(chunk, idx, label))}
      </div>
    )
  }

  // Calculate metrics when mlflowRuns changes
  useEffect(() => {
    if (!showResults || !mlflowRuns || mlflowRuns.length === 0) {
      setBuildMetrics([])
      setStrategyMetrics([])
      setEvaluationMetrics([])
      setBestPerformers({
        bestBuild: null,
        bestStrategy: null,
        fastest: null,
        bestOverall: null
      })
      return
    }

    // Use dynamic import to avoid circular dependency issues
    const calculateMetrics = async () => {
      try {
        console.log('[useEffect] Calculating metrics for', mlflowRuns.length, 'runs')
        
        // Dynamically import functions to break circular dependency
        const {
          aggregateByBuild,
          aggregateByStrategy,
          aggregateByEvaluation,
          calculateBestPerformers
        } = await import('../utils/metricsAggregation')

        const buildMetrics = aggregateByBuild(mlflowRuns)
        const strategyMetrics = aggregateByStrategy(mlflowRuns)
        const evaluationMetrics = aggregateByEvaluation(mlflowRuns)
        const bestPerformers = calculateBestPerformers(mlflowRuns)

        console.log('[useEffect] Calculated metrics:', {
          buildMetrics: buildMetrics.length,
          strategyMetrics: strategyMetrics.length,
          evaluationMetrics: evaluationMetrics.length,
          bestPerformers
        })

        setBuildMetrics(buildMetrics)
        setStrategyMetrics(strategyMetrics)
        setEvaluationMetrics(evaluationMetrics)
        setBestPerformers(bestPerformers)
      } catch (error) {
        console.error('Error calculating aggregated metrics:', error)
        setBuildMetrics([])
        setStrategyMetrics([])
        setEvaluationMetrics([])
        setBestPerformers({
          bestBuild: null,
          bestStrategy: null,
          fastest: null,
          bestOverall: null
        })
      }
    }

    calculateMetrics()
  }, [showResults, mlflowRuns])

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-databricks-gray-900">
            Review & Compare
          </h1>
          <p className="text-sm text-databricks-gray-600 mt-1">
            Select builds and evaluations to compare performance metrics
          </p>
        </div>
        <Button
          variant="outline"
          onClick={handleRefresh}
          icon={<RefreshCw className="w-4 h-4" />}
          disabled={!selectedProjectId}
        >
          Refresh
        </Button>
      </div>

      {!selectedProjectId && (
        <Card className="mb-6 bg-yellow-50 border-yellow-200">
          <p className="text-sm text-yellow-800">
            Please select a project from the Projects page to view and compare evaluations.
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

      {selectedProjectId && (
        <Card className="mb-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold text-databricks-gray-900">
                Review Scope
              </h3>
              <p className="text-xs text-databricks-gray-600">
                Choose how to aggregate evaluation runs for this project.
              </p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setReviewScope('project')}
                className={`px-3 py-2 text-xs font-medium rounded-md transition-colors ${
                  reviewScope === 'project'
                    ? 'bg-databricks-blue text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Project
              </button>
              <button
                onClick={() => setReviewScope('build')}
                className={`px-3 py-2 text-xs font-medium rounded-md transition-colors ${
                  reviewScope === 'build'
                    ? 'bg-databricks-blue text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Build
              </button>
              <button
                onClick={() => setReviewScope('evaluation')}
                className={`px-3 py-2 text-xs font-medium rounded-md transition-colors ${
                  reviewScope === 'evaluation'
                    ? 'bg-databricks-blue text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Evaluation
              </button>
            </div>
          </div>
        </Card>
      )}

      {error && (
        <Card className="mb-6 bg-red-50 border-red-200">
          <div className="p-4">
            <p className="text-sm text-red-800 font-medium mb-1">Error Loading Data</p>
            <p className="text-xs text-red-600">{error}</p>
            <button
              onClick={() => setError(null)}
              className="mt-2 text-xs text-red-600 hover:underline"
            >
              Dismiss
            </button>
          </div>
        </Card>
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

      {/* Step 1: Select Builds */}
      {reviewScope !== 'project' && (
        <BuildSelector
          builds={builds}
          selectedBuildIds={selectedBuildIds}
          onToggleBuild={handleToggleBuild}
          isLoading={isLoadingBuilds}
        />
      )}

      {/* Step 2: Select Evaluations */}
      {reviewScope === 'evaluation' && selectedBuildIds.size > 0 && (
        <EvaluationSelector
          evaluationsByBuild={evaluationsByBuild}
          selectedEvalIds={selectedEvalIds}
          onToggleEvaluation={handleToggleEvaluation}
          isLoading={isLoadingEvals}
        />
      )}

      {/* Review Button */}
      {reviewScope === 'project' && selectedProjectId && (
        <div className="mb-6 flex justify-center">
          <Button
            onClick={handleReviewClick}
            disabled={isLoadingMetrics}
            className="px-8 py-3 text-base"
          >
            {isLoadingMetrics ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Loading Metrics...
              </>
            ) : (
              <>
                🔍 Review Project
              </>
            )}
          </Button>
        </div>
      )}

      {reviewScope === 'build' && selectedBuildIds.size > 0 && (
        <div className="mb-6 flex justify-center">
          <Button
            onClick={handleReviewClick}
            disabled={isLoadingMetrics}
            className="px-8 py-3 text-base"
          >
            {isLoadingMetrics ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Loading Metrics...
              </>
            ) : (
              <>
                🔍 Review Selected ({selectedBuildIds.size} builds)
              </>
            )}
          </Button>
        </div>
      )}

      {reviewScope === 'evaluation' && selectedEvalIds.size > 0 && (
        <div className="mb-6 flex justify-center">
          <Button
            onClick={handleReviewClick}
            disabled={isLoadingMetrics}
            className="px-8 py-3 text-base"
          >
            {isLoadingMetrics ? (
              <>
                <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                Loading Metrics...
              </>
            ) : (
              <>
                🔍 Review Selected ({selectedEvalIds.size} evaluations)
              </>
            )}
          </Button>
        </div>
      )}

      {/* Results Section */}
      {showResults && mlflowRuns.length > 0 && (
        <>
          {bestPerformers && bestPerformers.bestBuild !== undefined && (
            <BestPerformers
              bestBuild={bestPerformers.bestBuild}
              bestStrategy={bestPerformers.bestStrategy}
              fastest={bestPerformers.fastest}
              bestOverall={bestPerformers.bestOverall}
            />
          )}

          <SourceComparison
            mlflowRuns={mlflowRuns}
            indexSelections={indexSelections}
            onUpdateIndexStatus={handleUpdateIndexStatus}
            onRequestExplanation={handleRequestExplanation}
          />

          {Array.isArray(buildMetrics) && Array.isArray(strategyMetrics) && Array.isArray(evaluationMetrics) && 
           (buildMetrics.length > 0 || strategyMetrics.length > 0 || evaluationMetrics.length > 0) ? (
            <>
              <MetricsBarCharts
                buildMetrics={buildMetrics || []}
                strategyMetrics={strategyMetrics || []}
                evaluationMetrics={evaluationMetrics || []}
              />

              {evaluationMetrics.length > 0 && (
                <ComparisonTable evaluationMetrics={evaluationMetrics} />
              )}

              {reviewScope === 'evaluation' && selectedEvalIds.size > 0 && (
                <Card className="mb-6">
                  <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
                    <div>
                      <h2 className="text-lg font-semibold text-databricks-gray-900">
                        Query Details
                      </h2>
                      <p className="text-xs text-databricks-gray-600">
                        Inspect expected vs retrieved chunks per query.
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      <select
                        className="text-xs border border-gray-300 rounded px-2 py-1"
                        value={selectedDetailsEvalId || ''}
                        onChange={(e) => setSelectedDetailsEvalId(e.target.value)}
                      >
                        {Array.from(selectedEvalIds).map((id) => (
                          <option key={id} value={id}>
                            {id.substring(0, 12)}...
                          </option>
                        ))}
                      </select>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setQueryPage(0)
                          setQuerySearch('')
                          setExpandedQueries(new Set())
                          if (selectedDetailsEvalId) loadQueryDetails(selectedDetailsEvalId)
                        }}
                        disabled={isLoadingDetails || !selectedDetailsEvalId}
                      >
                        {isLoadingDetails ? 'Loading...' : 'Load Details'}
                      </Button>
                    </div>
                  </div>

                  {queryDetails.length === 0 && !isLoadingDetails && (
                    <div className="text-center py-8">
                      {error ? (
                        <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-md text-left">
                          <p className="text-sm font-medium text-yellow-800 mb-1">No Results Available</p>
                          <p className="text-xs text-yellow-700">{error}</p>
                        </div>
                      ) : (
                        <p className="text-sm text-databricks-gray-600">
                          Click "Load Details" to fetch query-level results for the selected evaluation.
                        </p>
                      )}
                    </div>
                  )}

                  {queryDetails.length > 0 && (
                    <>
                      <div className="flex flex-wrap items-center gap-3 mb-4">
                        <div className="relative flex-1 min-w-[200px] max-w-md">
                          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
                          <input
                            type="text"
                            value={querySearch}
                            onChange={(e) => { setQuerySearch(e.target.value); setQueryPage(0) }}
                            placeholder="Search queries..."
                            className="w-full pl-8 pr-3 py-1.5 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-1 focus:ring-databricks-blue focus:border-databricks-blue"
                          />
                        </div>
                        <span className="text-xs text-gray-500">
                          {filteredQueryDetails.length} of {queryDetails.length} queries
                        </span>
                      </div>

                      <div className="space-y-3">
                        {pagedQueryDetails.map((row: any, pageIdx: number) => {
                          const globalIdx = queryPage * QUERIES_PER_PAGE + pageIdx
                          const isOpen = expandedQueries.has(globalIdx)
                          const metrics = row.metrics_parsed
                          return (
                            <div key={`${row.eval_result_id || globalIdx}`} className="border border-gray-200 rounded-xl overflow-hidden">
                              <button
                                onClick={() => toggleQuery(globalIdx)}
                                className="w-full flex items-start gap-3 p-3 text-left hover:bg-gray-50 transition-colors"
                              >
                                <div className="mt-0.5">
                                  {isOpen ? <ChevronUp className="w-4 h-4 text-gray-400" /> : <ChevronDown className="w-4 h-4 text-gray-400" />}
                                </div>
                                <div className="flex-1 min-w-0">
                                  <p className="text-sm font-medium text-gray-900 leading-snug">
                                    {row.query_text || 'Query'}
                                  </p>
                                  {metrics && (
                                    <div className="flex flex-wrap gap-2 mt-1.5">
                                      {metrics.recall_at_10 !== undefined && (
                                        <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${
                                          metrics.recall_at_10 >= 0.8 ? 'bg-green-100 text-green-700'
                                          : metrics.recall_at_10 >= 0.5 ? 'bg-yellow-100 text-yellow-700'
                                          : 'bg-red-100 text-red-700'
                                        }`}>R@10: {metrics.recall_at_10.toFixed(2)}</span>
                                      )}
                                      {metrics.ndcg_at_10 !== undefined && (
                                        <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${
                                          metrics.ndcg_at_10 >= 0.8 ? 'bg-green-100 text-green-700'
                                          : metrics.ndcg_at_10 >= 0.5 ? 'bg-yellow-100 text-yellow-700'
                                          : 'bg-red-100 text-red-700'
                                        }`}>N@10: {metrics.ndcg_at_10.toFixed(2)}</span>
                                      )}
                                      {metrics.precision_at_10 !== undefined && (
                                        <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-100 text-gray-600 font-medium">
                                          P@10: {metrics.precision_at_10.toFixed(2)}
                                        </span>
                                      )}
                                    </div>
                                  )}
                                </div>
                              </button>

                              {isOpen && (
                                <div className="border-t border-gray-100 p-4 bg-gray-50/50">
                                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                                    <div>
                                      <h4 className="text-xs font-semibold text-gray-700 mb-2 flex items-center gap-1.5">
                                        <span className="w-2 h-2 rounded-full bg-blue-500"></span>
                                        Expected Chunks
                                        {row.expected_chunks_parsed && <span className="text-gray-400 font-normal">({row.expected_chunks_parsed.length})</span>}
                                      </h4>
                                      {renderChunks(row.expected_chunks_parsed, `expected-${globalIdx}`)}
                                    </div>
                                    <div>
                                      <h4 className="text-xs font-semibold text-gray-700 mb-2 flex items-center gap-1.5">
                                        <span className="w-2 h-2 rounded-full bg-emerald-500"></span>
                                        Retrieved Chunks
                                        {row.retrieved_chunks_parsed && <span className="text-gray-400 font-normal">({row.retrieved_chunks_parsed.length})</span>}
                                      </h4>
                                      {renderChunks(row.retrieved_chunks_parsed, `retrieved-${globalIdx}`)}
                                    </div>
                                  </div>
                                </div>
                              )}
                            </div>
                          )
                        })}
                      </div>

                      {totalQueryPages > 1 && (
                        <div className="flex items-center justify-between mt-4 pt-3 border-t border-gray-200">
                          <Button variant="outline" size="sm" onClick={() => setQueryPage(p => Math.max(0, p - 1))} disabled={queryPage === 0}
                            icon={<ChevronLeft className="w-3.5 h-3.5" />}>
                            Previous
                          </Button>
                          <span className="text-xs text-gray-500">
                            Page {queryPage + 1} of {totalQueryPages}
                          </span>
                          <Button variant="outline" size="sm" onClick={() => setQueryPage(p => Math.min(totalQueryPages - 1, p + 1))} disabled={queryPage >= totalQueryPages - 1}>
                            Next <ChevronRight className="w-3.5 h-3.5 ml-1" />
                          </Button>
                        </div>
                      )}
                    </>
                  )}
                </Card>
              )}
            </>
          ) : (
            <Card className="mb-6">
              <div className="text-center py-12">
                <p className="text-sm text-databricks-gray-600 mb-2">
                  No metrics data available for visualization.
                </p>
                <p className="text-xs text-databricks-gray-500">
                  The runs may not have the expected metric structure.
                </p>
              </div>
            </Card>
          )}
        </>
      )}

      {showResults && mlflowRuns.length === 0 && (
        <Card className="mb-6">
          <div className="text-center py-12">
            <p className="text-sm text-databricks-gray-600 mb-2">
              No metrics found for the selected evaluations.
            </p>
            <p className="text-xs text-databricks-gray-500">
              Make sure the evaluation jobs have completed successfully.
            </p>
          </div>
        </Card>
      )}
    </div>
  )
}
