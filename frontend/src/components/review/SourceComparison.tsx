import { useState } from 'react'
import { ChevronDown, ChevronUp, Star, Lightbulb, Loader2 } from 'lucide-react'
import { Card } from '../ui/Card'
import { Badge } from '../ui/Badge'
import { Button } from '../ui/Button'
import type { MLflowRun } from '../../utils/metricsAggregation'

interface SourceGroup {
  sourceName: string
  sourceType: string
  strategies: {
    strategyName: string
    queryType: string
    metrics: Record<string, number>
    runId: string
  }[]
  bestStrategy: string | null
}

interface IndexSelection {
  id: string
  source_name: string
  strategy_name: string
  index_name: string
  chunks_table: string
  status: string
}

interface Props {
  mlflowRuns: MLflowRun[]
  indexSelections?: IndexSelection[]
  onUpdateIndexStatus?: (updates: { id: string; status: string }[]) => void
  onRequestExplanation?: (sourceName: string, strategies: any[]) => Promise<string>
}

function groupBySource(runs: MLflowRun[]): SourceGroup[] {
  const groups: Record<string, SourceGroup> = {}

  for (const run of runs) {
    if (run.role !== 'eval_strategy') continue
    const sourceName = run.params?.source_name || 'all_sources'
    const sourceType = run.params?.source_type || ''
    const strategyName = run.params?.strategy_name || 'unknown'
    const queryType = run.params?.query_type || 'ANN'

    if (!groups[sourceName]) {
      groups[sourceName] = { sourceName, sourceType, strategies: [], bestStrategy: null }
    }

    groups[sourceName].strategies.push({
      strategyName,
      queryType,
      metrics: run.metrics || {},
      runId: run.run_id || '',
    })
  }

  for (const group of Object.values(groups)) {
    let bestRecall = -1
    for (const s of group.strategies) {
      const recall = s.metrics.recall_at_10 || 0
      if (recall > bestRecall) {
        bestRecall = recall
        group.bestStrategy = s.strategyName
      }
    }
  }

  return Object.values(groups).sort((a, b) => a.sourceName.localeCompare(b.sourceName))
}

const METRIC_KEYS = ['recall_at_10', 'ndcg_at_10', 'precision_at_10', 'avg_latency_ms'] as const

export default function SourceComparison({ mlflowRuns, indexSelections = [], onUpdateIndexStatus, onRequestExplanation }: Props) {
  const [expandedSources, setExpandedSources] = useState<Set<string>>(new Set())
  const [explanations, setExplanations] = useState<Record<string, string>>({})
  const [loadingExplanation, setLoadingExplanation] = useState<string | null>(null)
  const [pendingStatuses, setPendingStatuses] = useState<Record<string, string>>({})

  const sourceGroups = groupBySource(mlflowRuns)

  if (sourceGroups.length === 0) return null

  const hasPerSourceData = sourceGroups.some(g => g.sourceName !== 'all_sources')
  if (!hasPerSourceData) return null

  const toggleSource = (name: string) => {
    setExpandedSources(prev => {
      const n = new Set(prev)
      n.has(name) ? n.delete(name) : n.add(name)
      return n
    })
  }

  const handleExplanation = async (sourceName: string, strategies: any[]) => {
    if (!onRequestExplanation) return
    setLoadingExplanation(sourceName)
    try {
      const explanation = await onRequestExplanation(sourceName, strategies)
      setExplanations(prev => ({ ...prev, [sourceName]: explanation }))
    } catch {
      setExplanations(prev => ({ ...prev, [sourceName]: 'Failed to generate explanation.' }))
    }
    setLoadingExplanation(null)
  }

  const toggleIndexStatus = (selectionId: string, currentStatus: string) => {
    const newStatus = currentStatus === 'keep' ? 'discard' : 'keep'
    setPendingStatuses(prev => ({ ...prev, [selectionId]: newStatus }))
  }

  const saveSelections = () => {
    if (!onUpdateIndexStatus) return
    const updates = Object.entries(pendingStatuses).map(([id, status]) => ({ id, status }))
    if (updates.length > 0) {
      onUpdateIndexStatus(updates)
      setPendingStatuses({})
    }
  }

  const getSelectionForSourceStrategy = (sourceName: string, strategyName: string): IndexSelection | undefined => {
    return indexSelections.find(s => s.source_name === sourceName && s.strategy_name === strategyName)
  }

  const getEffectiveStatus = (selection: IndexSelection | undefined): string => {
    if (!selection) return 'active'
    return pendingStatuses[selection.id] ?? selection.status
  }

  const formatMetric = (value: number | undefined, key: string): string => {
    if (value === undefined || value === null) return '-'
    if (key === 'avg_latency_ms') return `${Math.round(value)}ms`
    return value.toFixed(3)
  }

  const metricColor = (value: number | undefined, key: string): string => {
    if (value === undefined) return ''
    if (key === 'avg_latency_ms') {
      if (value < 100) return 'text-green-700 bg-green-50'
      if (value < 500) return 'text-yellow-700 bg-yellow-50'
      return 'text-red-700 bg-red-50'
    }
    if (value >= 0.8) return 'text-green-700 bg-green-50'
    if (value >= 0.5) return 'text-yellow-700 bg-yellow-50'
    return 'text-red-700 bg-red-50'
  }

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold text-databricks-gray-900">Per-Source Comparison</h2>
          <p className="text-xs text-databricks-gray-600">Compare strategies within each data source. Select indexes to keep.</p>
        </div>
        {Object.keys(pendingStatuses).length > 0 && onUpdateIndexStatus && (
          <Button variant="primary" size="sm" onClick={saveSelections}>
            Save Selections ({Object.keys(pendingStatuses).length})
          </Button>
        )}
      </div>

      <div className="space-y-4">
        {sourceGroups.filter(g => g.sourceName !== 'all_sources').map(group => {
          const isExpanded = expandedSources.has(group.sourceName)
          return (
            <div key={group.sourceName} className="border border-databricks-gray-200 rounded-lg overflow-hidden">
              <button onClick={() => toggleSource(group.sourceName)}
                className="w-full flex items-center justify-between p-4 bg-databricks-gray-50 hover:bg-databricks-gray-100 transition-colors">
                <div className="flex items-center gap-3">
                  <h3 className="text-sm font-semibold text-databricks-gray-900">{group.sourceName}</h3>
                  {group.sourceType && <Badge variant="secondary">{group.sourceType}</Badge>}
                  <Badge variant="info">{group.strategies.length} strateg{group.strategies.length !== 1 ? 'ies' : 'y'}</Badge>
                </div>
                <div className="flex items-center gap-3">
                  {group.bestStrategy && (
                    <div className="flex items-center gap-1 text-xs text-green-700">
                      <Star className="w-3.5 h-3.5" /> Best: {group.bestStrategy}
                    </div>
                  )}
                  {isExpanded ? <ChevronUp className="w-4 h-4 text-databricks-gray-400" /> : <ChevronDown className="w-4 h-4 text-databricks-gray-400" />}
                </div>
              </button>

              {isExpanded && (
                <div className="p-4">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-databricks-gray-200">
                        <th className="text-left py-2 px-2 text-databricks-gray-700">Strategy</th>
                        <th className="text-left py-2 px-2 text-databricks-gray-700">Query Type</th>
                        {METRIC_KEYS.map(k => <th key={k} className="text-center py-2 px-2 text-databricks-gray-700">{k.replace(/_/g, ' ').replace('at ', '@')}</th>)}
                        {indexSelections.length > 0 && <th className="text-center py-2 px-2 text-databricks-gray-700">Keep?</th>}
                      </tr>
                    </thead>
                    <tbody>
                      {group.strategies.map((s, i) => {
                        const isBest = s.strategyName === group.bestStrategy
                        const selection = getSelectionForSourceStrategy(group.sourceName, s.strategyName)
                        const effectiveStatus = getEffectiveStatus(selection)
                        return (
                          <tr key={i} className={`border-b border-databricks-gray-100 ${isBest ? 'bg-green-50/50' : ''}`}>
                            <td className="py-2 px-2 font-medium">
                              <div className="flex items-center gap-1.5">
                                {isBest && <Star className="w-3.5 h-3.5 text-yellow-500" />}
                                {s.strategyName}
                              </div>
                            </td>
                            <td className="py-2 px-2"><Badge variant="secondary">{s.queryType}</Badge></td>
                            {METRIC_KEYS.map(k => (
                              <td key={k} className="py-2 px-2 text-center">
                                <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${metricColor(s.metrics[k], k)}`}>
                                  {formatMetric(s.metrics[k], k)}
                                </span>
                              </td>
                            ))}
                            {indexSelections.length > 0 && (
                              <td className="py-2 px-2 text-center">
                                {selection && (
                                  <button
                                    onClick={() => toggleIndexStatus(selection.id, effectiveStatus)}
                                    className={`px-2 py-1 text-xs font-medium rounded transition-colors ${
                                      effectiveStatus === 'keep' ? 'bg-green-100 text-green-800 hover:bg-green-200'
                                      : effectiveStatus === 'discard' ? 'bg-red-100 text-red-800 hover:bg-red-200'
                                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                    }`}
                                  >
                                    {effectiveStatus === 'keep' ? 'Keep' : effectiveStatus === 'discard' ? 'Discard' : 'Select'}
                                  </button>
                                )}
                              </td>
                            )}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>

                  <div className="mt-3 flex items-center gap-3">
                    {onRequestExplanation && (
                      <Button variant="outline" size="sm"
                        disabled={loadingExplanation === group.sourceName}
                        onClick={() => handleExplanation(group.sourceName, group.strategies)}>
                        {loadingExplanation === group.sourceName ? (
                          <><Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" /> Generating...</>
                        ) : (
                          <><Lightbulb className="w-3.5 h-3.5 mr-1.5" /> Why is {group.bestStrategy} best?</>
                        )}
                      </Button>
                    )}
                  </div>

                  {explanations[group.sourceName] && (
                    <div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-md">
                      <p className="text-xs font-medium text-blue-900 mb-1">Strategy Analysis</p>
                      <p className="text-xs text-blue-800 leading-relaxed">{explanations[group.sourceName]}</p>
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </Card>
  )
}
