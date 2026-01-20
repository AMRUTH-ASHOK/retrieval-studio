import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw } from 'lucide-react'
import { useProject } from '../context/ProjectContext'
import { buildsApi } from '../services/builds'
import { evaluationsApi } from '../services/evaluations'
import { projectsApi } from '../services/projects'
import { BuildJob, Evaluation } from '../types'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import BuildSelector from '../components/review/BuildSelector'
import EvaluationSelector from '../components/review/EvaluationSelector'
import BestPerformers from '../components/review/BestPerformers'
import MetricsBarCharts from '../components/review/MetricsBarCharts'
import ComparisonTable from '../components/review/ComparisonTable'
import type { MLflowRun } from '../utils/metricsAggregation'

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

  // Load evaluations when selected builds change
  useEffect(() => {
    if (selectedBuildIds.size > 0) {
      loadEvaluationsForSelectedBuilds()
    } else {
      setEvaluationsByBuild(new Map())
      setSelectedEvalIds(new Set())
      setShowResults(false)
    }
  }, [selectedBuildIds])

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

  const loadMetricsForSelectedEvaluations = async () => {
    if (!selectedProjectId || selectedEvalIds.size === 0) return

    setIsLoadingMetrics(true)
    try {
      // Get the build IDs for the selected evaluations
      const selectedBuildIdsForEvals = new Set<string>()
      selectedEvalIds.forEach(evalId => {
        // Find which build this evaluation belongs to
        for (const [buildId, evals] of evaluationsByBuild.entries()) {
          if (evals.some(e => e.run_id === evalId)) {
            selectedBuildIdsForEvals.add(buildId)
            break
          }
        }
      })

      console.log('[loadMetrics] Selected eval IDs:', Array.from(selectedEvalIds))
      console.log('[loadMetrics] Selected build IDs for evals:', Array.from(selectedBuildIdsForEvals))

      // Load all MLflow runs for the project
      const runsData = await projectsApi.getMLflowRuns(selectedProjectId)
      
      // Validate response structure
      if (!runsData || !Array.isArray(runsData.runs)) {
        console.error('Invalid MLflow runs response:', runsData)
        setMlflowRuns([])
        setShowResults(false)
        return
      }

      console.log('[loadMetrics] Total MLflow runs:', runsData.runs.length)
      console.log('[loadMetrics] All runs roles:', runsData.runs.map(r => ({ run_id: r.run_id, role: r.role, build_run_id: r.params?.build_run_id || r.tags?.build_run_id })))

      // Filter to only eval_strategy runs that belong to the selected builds
      const filteredRuns = runsData.runs.filter((run: MLflowRun) => {
        try {
          if (!run) {
            console.log('[loadMetrics] Skipping null run')
            return false
          }
          
          if (run.role !== 'eval_strategy') {
            console.log('[loadMetrics] Skipping non-eval_strategy run:', run.role)
            return false
          }

          // Check if this MLflow run belongs to one of our selected builds
          const buildRunId = run.params?.build_run_id || run.tags?.build_run_id
          console.log('[loadMetrics] Checking run:', {
            run_id: run.run_id,
            build_run_id: buildRunId,
            has_build_id: !!buildRunId,
            in_selected: buildRunId ? selectedBuildIdsForEvals.has(buildRunId) : false,
            selected_builds: Array.from(selectedBuildIdsForEvals)
          })
          
          if (!buildRunId) {
            console.log('[loadMetrics] Run has no build_run_id:', run)
            return false
          }
          
          const matches = selectedBuildIdsForEvals.has(buildRunId)
          if (matches) {
            console.log('[loadMetrics] ✅ Run matches:', run.run_id, 'build:', buildRunId)
          }
          return matches
        } catch (e) {
          console.warn('Error filtering run:', e, run)
          return false
        }
      })

      console.log('Selected eval IDs:', Array.from(selectedEvalIds))
      console.log('Selected build IDs for evals:', Array.from(selectedBuildIdsForEvals))
      console.log('Total MLflow runs:', runsData.runs.length)
      console.log('Filtered MLflow runs:', filteredRuns.length)
      console.log('Sample run structure:', filteredRuns[0])
      console.log('Sample run metrics:', filteredRuns[0]?.metrics)
      console.log('Sample run params:', filteredRuns[0]?.params)
      console.log('Sample run tags:', filteredRuns[0]?.tags)
      console.log('All filtered runs:', filteredRuns.map(r => ({
        run_id: r.run_id,
        role: r.role,
        metrics: r.metrics,
        params: r.params,
        tags: r.tags
      })))

      setMlflowRuns(filteredRuns)
      setShowResults(true)
      setError(null)
    } catch (error: any) {
      console.error('Failed to load metrics:', error)
      setMlflowRuns([])
      setShowResults(false)
      setError(error?.message || 'Failed to load metrics. Please check the console for details.')
    } finally {
      setIsLoadingMetrics(false)
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
    loadMetricsForSelectedEvaluations()
  }

  const handleRefresh = () => {
    loadBuilds()
    loadMLflowExperiment()
    if (selectedBuildIds.size > 0) {
      loadEvaluationsForSelectedBuilds()
    }
    if (selectedEvalIds.size > 0) {
      loadMetricsForSelectedEvaluations()
    }
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
      <BuildSelector
        builds={builds}
        selectedBuildIds={selectedBuildIds}
        onToggleBuild={handleToggleBuild}
        isLoading={isLoadingBuilds}
      />

      {/* Step 2: Select Evaluations */}
      {selectedBuildIds.size > 0 && (
        <EvaluationSelector
          evaluationsByBuild={evaluationsByBuild}
          selectedEvalIds={selectedEvalIds}
          onToggleEvaluation={handleToggleEvaluation}
          isLoading={isLoadingEvals}
        />
      )}

      {/* Review Button */}
      {selectedEvalIds.size > 0 && (
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
