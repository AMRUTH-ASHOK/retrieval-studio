/**
 * Metrics Aggregation Utilities for Review Page
 * Processes MLflow runs data for visualization and comparison
 */

export type MLflowRun = {
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

export type AggregatedMetrics = {
  recall_at_5?: number
  recall_at_10?: number
  ndcg_at_5?: number
  ndcg_at_10?: number
  avg_relevance_at_5?: number
  avg_relevance_at_10?: number
  avg_latency_ms?: number
  num_queries?: number
}

export type BuildMetrics = {
  build_run_id: string
  build_name: string
  metrics: AggregatedMetrics
  strategy_count: number
}

export type StrategyMetrics = {
  strategy: string
  build_run_id?: string
  metrics: AggregatedMetrics
  query_type?: string
}

export type EvaluationMetrics = {
  eval_run_id: string
  eval_name: string
  build_run_id: string
  strategy: string
  query_type: string
  metrics: AggregatedMetrics
}

export interface BestPerformer {
  name: string
  metric_value: number
  metric_name: string
  type: 'build' | 'strategy' | 'evaluation'
  id: string
}

/**
 * Aggregate metrics by build run ID
 */
export function aggregateByBuild(mlflowRuns: MLflowRun[]): BuildMetrics[] {
  if (!Array.isArray(mlflowRuns) || mlflowRuns.length === 0) {
    console.log('[aggregateByBuild] No runs provided')
    return []
  }

  // Group eval_strategy runs by build_run_id
  const evalRuns = mlflowRuns.filter(r => r && r.role === 'eval_strategy')
  console.log('[aggregateByBuild] Filtered eval_strategy runs:', evalRuns.length)

  const buildMap = new Map<string, MLflowRun[]>()

  evalRuns.forEach(run => {
    if (!run) return
    const buildRunId = run.params?.build_run_id || run.tags?.build_run_id
    console.log('[aggregateByBuild] Processing run:', {
      run_id: run.run_id,
      build_run_id: buildRunId,
      metrics: run.metrics,
      params: run.params,
      tags: run.tags
    })
    if (buildRunId) {
      if (!buildMap.has(buildRunId)) {
        buildMap.set(buildRunId, [])
      }
      buildMap.get(buildRunId)!.push(run)
    }
  })
  
  console.log('[aggregateByBuild] Build map size:', buildMap.size)

  const buildMetrics: BuildMetrics[] = []

  buildMap.forEach((runs, buildRunId) => {
    const metricsArray = runs.map(r => r.metrics || {})
    console.log('[aggregateByBuild] Averaging metrics for build:', buildRunId, 'from runs:', metricsArray)
    const avgMetrics = averageMetrics(metricsArray)
    console.log('[aggregateByBuild] Averaged metrics:', avgMetrics)

    buildMetrics.push({
      build_run_id: buildRunId,
      build_name: `Build ${buildRunId.substring(0, 8)}`,
      metrics: avgMetrics,
      strategy_count: new Set(runs.map(r => r.tags?.strategy || r.params?.strategy_name || r.params?.strategy)).size
    })
  })
  
  console.log('[aggregateByBuild] Final buildMetrics:', buildMetrics)

  return buildMetrics.sort((a, b) =>
    (b.metrics.recall_at_10 || 0) - (a.metrics.recall_at_10 || 0)
  )
}

/**
 * Aggregate metrics by strategy
 */
export function aggregateByStrategy(mlflowRuns: MLflowRun[]): StrategyMetrics[] {
  if (!Array.isArray(mlflowRuns) || mlflowRuns.length === 0) {
    return []
  }

  // Group eval_strategy runs by strategy name
  const evalRuns = mlflowRuns.filter(r => r && r.role === 'eval_strategy')

  const strategyMap = new Map<string, MLflowRun[]>()

  evalRuns.forEach(run => {
    if (!run) return
    const strategy = run.tags?.strategy || run.params?.strategy_name || run.params?.strategy || 'unknown'
    if (!strategyMap.has(strategy)) {
      strategyMap.set(strategy, [])
    }
    strategyMap.get(strategy)!.push(run)
  })

  const strategyMetrics: StrategyMetrics[] = []

  strategyMap.forEach((runs, strategy) => {
    const avgMetrics = averageMetrics(runs.map(r => r.metrics))

    strategyMetrics.push({
      strategy,
      metrics: avgMetrics
    })
  })

  return strategyMetrics.sort((a, b) =>
    (b.metrics.ndcg_at_5 || 0) - (a.metrics.ndcg_at_5 || 0)
  )
}

/**
 * Get individual evaluation metrics
 */
export function aggregateByEvaluation(mlflowRuns: MLflowRun[]): EvaluationMetrics[] {
  if (!Array.isArray(mlflowRuns) || mlflowRuns.length === 0) {
    return []
  }

  const evalRuns = mlflowRuns.filter(r => r && r.role === 'eval_strategy')

  return evalRuns.map(run => {
    if (!run) {
      return null as any
    }
    return {
      eval_run_id: run.run_id || 'unknown',
      eval_name: run.run_name || 'Unnamed Run',
      build_run_id: run.params?.build_run_id || run.tags?.build_run_id || 'unknown',
      strategy: run.tags?.strategy || run.params?.strategy_name || run.params?.strategy || 'unknown',
      query_type: run.tags?.query_type || run.params?.query_type || 'unknown',
      metrics: (run.metrics || {}) as AggregatedMetrics
    }
  }).filter(r => r !== null).sort((a, b) =>
    (b.metrics.recall_at_10 || 0) - (a.metrics.recall_at_10 || 0)
  )
}

/**
 * Calculate best performers across all dimensions
 */
export function calculateBestPerformers(mlflowRuns: MLflowRun[]): {
  bestBuild: BestPerformer | null
  bestStrategy: BestPerformer | null
  fastest: BestPerformer | null
  bestOverall: BestPerformer | null
} {
  if (!Array.isArray(mlflowRuns) || mlflowRuns.length === 0) {
    return {
      bestBuild: null,
      bestStrategy: null,
      fastest: null,
      bestOverall: null
    }
  }

  let buildMetrics: BuildMetrics[] = []
  let strategyMetrics: StrategyMetrics[] = []
  let evalMetrics: EvaluationMetrics[] = []

  try {
    buildMetrics = aggregateByBuild(mlflowRuns)
    strategyMetrics = aggregateByStrategy(mlflowRuns)
    evalMetrics = aggregateByEvaluation(mlflowRuns)
  } catch (error) {
    console.error('Error in calculateBestPerformers:', error)
    return {
      bestBuild: null,
      bestStrategy: null,
      fastest: null,
      bestOverall: null
    }
  }

  // Best build by Recall@10
  const bestBuild = buildMetrics[0]
    ? {
        name: buildMetrics[0].build_name,
        metric_value: buildMetrics[0].metrics.recall_at_10 || 0,
        metric_name: 'Recall@10',
        type: 'build' as const,
        id: buildMetrics[0].build_run_id
      }
    : null

  // Best strategy by NDCG@5
  const bestStrategy = strategyMetrics[0]
    ? {
        name: strategyMetrics[0].strategy,
        metric_value: strategyMetrics[0].metrics.ndcg_at_5 || 0,
        metric_name: 'NDCG@5',
        type: 'strategy' as const,
        id: strategyMetrics[0].strategy
      }
    : null

  // Fastest by latency
  const sortedByLatency = [...evalMetrics].sort((a, b) =>
    (a.metrics.avg_latency_ms || Infinity) - (b.metrics.avg_latency_ms || Infinity)
  )
  const fastest = sortedByLatency[0]
    ? {
        name: `${sortedByLatency[0].strategy}`,
        metric_value: sortedByLatency[0].metrics.avg_latency_ms || 0,
        metric_name: 'Latency',
        type: 'evaluation' as const,
        id: sortedByLatency[0].eval_run_id
      }
    : null

  // Best overall: balanced score (weighted average of quality and speed)
  const overallScores = evalMetrics.map(evaluation => {
    const recall = evaluation.metrics.recall_at_10 || 0
    const ndcg = evaluation.metrics.ndcg_at_5 || 0
    const latency = evaluation.metrics.avg_latency_ms || 1000

    // Normalize latency (lower is better): 0-100ms = 1.0, 1000ms+ = 0.0
    const normalizedLatency = Math.max(0, 1 - (latency / 1000))

    // Weighted score: 40% recall, 40% ndcg, 20% speed
    const score = (recall * 0.4) + (ndcg * 0.4) + (normalizedLatency * 0.2)

    return { evaluation, score }
  }).sort((a, b) => b.score - a.score)

  const bestOverall = overallScores[0]
    ? {
        name: `${overallScores[0].evaluation.strategy} (${overallScores[0].evaluation.query_type})`,
        metric_value: overallScores[0].score * 10, // Scale to 0-10
        metric_name: 'Overall Score',
        type: 'evaluation' as const,
        id: overallScores[0].evaluation.eval_run_id
      }
    : null

  return { bestBuild, bestStrategy, fastest, bestOverall }
}

/**
 * Format data for Plotly bar charts
 */
export function formatForBarChart(
  data: BuildMetrics[] | StrategyMetrics[] | EvaluationMetrics[],
  groupBy: 'build' | 'strategy' | 'evaluation',
  metricKey: keyof AggregatedMetrics
): { x: string[], y: number[], text: string[] } {
  let labels: string[] = []
  let values: number[] = []

  if (groupBy === 'build') {
    const buildData = data as BuildMetrics[]
    labels = buildData.map(d => d.build_name)
    values = buildData.map(d => d.metrics[metricKey] as number || 0)
  } else if (groupBy === 'strategy') {
    const stratData = data as StrategyMetrics[]
    labels = stratData.map(d => d.strategy)
    values = stratData.map(d => d.metrics[metricKey] as number || 0)
  } else {
    const evalData = data as EvaluationMetrics[]
    labels = evalData.map(d => `${d.strategy} (${d.query_type})`)
    values = evalData.map(d => d.metrics[metricKey] as number || 0)
  }

  const text = values.map(v => formatMetricValue(v, metricKey))

  return { x: labels, y: values, text }
}

/**
 * Average metrics across multiple runs
 */
function averageMetrics(metricsArray: Record<string, number>[]): AggregatedMetrics {
  if (metricsArray.length === 0) {
    console.log('[averageMetrics] Empty metrics array')
    return {}
  }

  console.log('[averageMetrics] Input metrics array:', metricsArray)
  
  const sums: Record<string, number> = {}
  const counts: Record<string, number> = {}

  metricsArray.forEach((metrics, idx) => {
    if (!metrics || typeof metrics !== 'object') {
      console.log(`[averageMetrics] Skipping invalid metrics at index ${idx}:`, metrics)
      return
    }
    
    Object.entries(metrics).forEach(([key, value]) => {
      if (typeof value === 'number' && !isNaN(value) && isFinite(value)) {
        sums[key] = (sums[key] || 0) + value
        counts[key] = (counts[key] || 0) + 1
      } else {
        console.log(`[averageMetrics] Skipping invalid metric ${key}:`, value, typeof value)
      }
    })
  })

  const averages: AggregatedMetrics = {}
  Object.keys(sums).forEach(key => {
    (averages as any)[key] = sums[key] / counts[key]
  })

  console.log('[averageMetrics] Calculated averages:', averages)
  return averages
}

/**
 * Format metric value for display
 */
export function formatMetricValue(value: number, metricKey: keyof AggregatedMetrics): string {
  if (metricKey === 'avg_latency_ms') {
    return `${Math.round(value)}ms`
  } else if (metricKey === 'num_queries') {
    return `${Math.round(value)}`
  } else {
    return value.toFixed(3)
  }
}

/**
 * Get color for metric value (for table cells)
 */
export function getMetricColor(value: number, metricKey: keyof AggregatedMetrics): string {
  if (metricKey === 'avg_latency_ms') {
    if (value < 100) return 'text-green-600 bg-green-50'
    if (value < 500) return 'text-yellow-600 bg-yellow-50'
    return 'text-red-600 bg-red-50'
  } else {
    // Quality metrics (recall, ndcg, relevance)
    if (value >= 0.8) return 'text-green-600 bg-green-50'
    if (value >= 0.5) return 'text-yellow-600 bg-yellow-50'
    return 'text-red-600 bg-red-50'
  }
}
