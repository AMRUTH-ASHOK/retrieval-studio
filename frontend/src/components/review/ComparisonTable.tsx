import { useState, useMemo } from 'react'
import { ArrowUpDown, ArrowUp, ArrowDown, Download, Filter, X } from 'lucide-react'
import { Card } from '../ui/Card'
import { Button } from '../ui/Button'
import { EvaluationMetrics, AggregatedMetrics, formatMetricValue, getMetricColor } from '../../utils/metricsAggregation'

interface ComparisonTableProps {
  evaluationMetrics: EvaluationMetrics[]
}

type SortColumn = 'eval_name' | 'build_run_id' | 'strategy' | 'query_type' |
  'precision_at_5' | 'precision_at_10' | 'recall_at_5' | 'recall_at_10' |
  'ndcg_at_5' | 'ndcg_at_10' | 'avg_latency_ms' | 'num_queries'

type MetricKey = keyof AggregatedMetrics

interface MetricFilter {
  key: MetricKey
  op: 'gt' | 'lt'
  value: number
}

const METRIC_COLUMNS: Array<{ key: MetricKey; label: string; shortLabel: string }> = [
  { key: 'precision_at_5', label: 'Precision@5', shortLabel: 'P@5' },
  { key: 'precision_at_10', label: 'Precision@10', shortLabel: 'P@10' },
  { key: 'recall_at_5', label: 'Recall@5', shortLabel: 'R@5' },
  { key: 'recall_at_10', label: 'Recall@10', shortLabel: 'R@10' },
  { key: 'ndcg_at_5', label: 'NDCG@5', shortLabel: 'N@5' },
  { key: 'ndcg_at_10', label: 'NDCG@10', shortLabel: 'N@10' },
  { key: 'avg_latency_ms', label: 'Latency', shortLabel: 'Lat.' },
]

export default function ComparisonTable({ evaluationMetrics }: ComparisonTableProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>('recall_at_10')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')
  const [filters, setFilters] = useState<MetricFilter[]>([])
  const [showFilterBar, setShowFilterBar] = useState(false)
  const [filterMetric, setFilterMetric] = useState<MetricKey>('recall_at_10')
  const [filterOp, setFilterOp] = useState<'gt' | 'lt'>('gt')
  const [filterValue, setFilterValue] = useState('')

  const bestValues = useMemo(() => {
    const best: Record<string, number> = {}
    METRIC_COLUMNS.forEach(({ key }) => {
      const isLatency = key === 'avg_latency_ms'
      const values = evaluationMetrics
        .map(e => e.metrics[key] as number)
        .filter(v => typeof v === 'number' && !isNaN(v) && v > 0)
      if (values.length > 0) {
        best[key] = isLatency ? Math.min(...values) : Math.max(...values)
      }
    })
    return best
  }, [evaluationMetrics])

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc')
    } else {
      setSortColumn(column)
      setSortDirection('desc')
    }
  }

  const addFilter = () => {
    const numVal = parseFloat(filterValue)
    if (isNaN(numVal)) return
    setFilters(prev => [...prev, { key: filterMetric, op: filterOp, value: numVal }])
    setFilterValue('')
  }

  const removeFilter = (idx: number) => {
    setFilters(prev => prev.filter((_, i) => i !== idx))
  }

  const filteredEvaluations = useMemo(() => {
    return evaluationMetrics.filter(evaluation => {
      return filters.every(f => {
        const val = evaluation.metrics[f.key] as number || 0
        return f.op === 'gt' ? val > f.value : val < f.value
      })
    })
  }, [evaluationMetrics, filters])

  const sortedEvaluations = useMemo(() => {
    return [...filteredEvaluations].sort((a, b) => {
      let aVal: any, bVal: any
      if (sortColumn === 'eval_name' || sortColumn === 'build_run_id' ||
          sortColumn === 'strategy' || sortColumn === 'query_type') {
        aVal = a[sortColumn]
        bVal = b[sortColumn]
      } else {
        aVal = a.metrics[sortColumn] || 0
        bVal = b.metrics[sortColumn] || 0
      }
      if (typeof aVal === 'string') {
        return sortDirection === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDirection === 'asc' ? aVal - bVal : bVal - aVal
    })
  }, [filteredEvaluations, sortColumn, sortDirection])

  const exportToCSV = () => {
    const headers = ['Evaluation ID', 'Build ID', 'Strategy', 'Query Type',
      'Precision@5', 'Precision@10', 'Recall@5', 'Recall@10', 'NDCG@5', 'NDCG@10', 'Latency (ms)', 'Num Queries']
    const rows = sortedEvaluations.map(e => [
      `"${e.eval_run_id}"`, `"${e.build_run_id}"`, `"${e.strategy}"`, `"${e.query_type}"`,
      e.metrics.precision_at_5 || '', e.metrics.precision_at_10 || '',
      e.metrics.recall_at_5 || '', e.metrics.recall_at_10 || '',
      e.metrics.ndcg_at_5 || '', e.metrics.ndcg_at_10 || '',
      e.metrics.avg_latency_ms || '', e.metrics.num_queries || ''
    ].join(','))
    const csv = [headers.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `evaluation-comparison-${Date.now()}.csv`
    a.click()
    window.URL.revokeObjectURL(url)
  }

  const SortIcon = ({ column }: { column: SortColumn }) => {
    if (sortColumn !== column) return <ArrowUpDown className="w-3 h-3 inline ml-0.5 text-gray-300" />
    return sortDirection === 'asc'
      ? <ArrowUp className="w-3 h-3 inline ml-0.5 text-databricks-blue" />
      : <ArrowDown className="w-3 h-3 inline ml-0.5 text-databricks-blue" />
  }

  if (evaluationMetrics.length === 0) {
    return (
      <Card className="mb-6">
        <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">Detailed Comparison</h2>
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">No evaluation data to display.</p>
        </div>
      </Card>
    )
  }

  const isBest = (metricKey: MetricKey, value: number | undefined) => {
    if (value === undefined || value === 0) return false
    return value === bestValues[metricKey]
  }

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
        <h2 className="text-lg font-semibold text-databricks-gray-900">
          Detailed Comparison
        </h2>
        <div className="flex items-center gap-2">
          <Button
            variant="outline" size="sm"
            onClick={() => setShowFilterBar(!showFilterBar)}
            icon={<Filter className="w-3.5 h-3.5" />}
            className={filters.length > 0 ? 'border-databricks-blue text-databricks-blue' : ''}
          >
            Filter{filters.length > 0 ? ` (${filters.length})` : ''}
          </Button>
          <Button variant="outline" size="sm" onClick={exportToCSV} icon={<Download className="w-3.5 h-3.5" />}>
            Export CSV
          </Button>
        </div>
      </div>

      {showFilterBar && (
        <div className="mb-4 p-3 bg-gray-50 rounded-lg border border-gray-200 space-y-2">
          <div className="flex items-center gap-2 flex-wrap">
            <select value={filterMetric} onChange={e => setFilterMetric(e.target.value as MetricKey)}
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 bg-white">
              {METRIC_COLUMNS.map(m => <option key={m.key} value={m.key}>{m.label}</option>)}
            </select>
            <select value={filterOp} onChange={e => setFilterOp(e.target.value as 'gt' | 'lt')}
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 bg-white">
              <option value="gt">&gt;</option>
              <option value="lt">&lt;</option>
            </select>
            <input type="number" step="0.01" value={filterValue}
              onChange={e => setFilterValue(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && addFilter()}
              placeholder="Value"
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 w-24 bg-white"
            />
            <Button variant="primary" size="sm" onClick={addFilter} disabled={!filterValue}>Add</Button>
          </div>
          {filters.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              {filters.map((f, i) => (
                <span key={i} className="inline-flex items-center gap-1 px-2 py-1 bg-databricks-blue/10 text-databricks-blue text-xs rounded-full">
                  {METRIC_COLUMNS.find(m => m.key === f.key)?.shortLabel} {f.op === 'gt' ? '>' : '<'} {f.value}
                  <button onClick={() => removeFilter(i)} className="hover:text-red-500"><X className="w-3 h-3" /></button>
                </span>
              ))}
              <button onClick={() => setFilters([])} className="text-xs text-gray-500 hover:text-red-500">Clear all</button>
            </div>
          )}
        </div>
      )}

      <div className="overflow-x-auto max-h-[500px] overflow-y-auto relative">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50 sticky top-0 z-10">
            <tr>
              {[
                { col: 'strategy' as SortColumn, label: 'Strategy' },
                { col: 'query_type' as SortColumn, label: 'Query Type' },
                { col: 'build_run_id' as SortColumn, label: 'Build' },
              ].map(({ col, label }) => (
                <th key={col} scope="col"
                  className="px-3 py-2.5 text-left text-[11px] font-semibold text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 bg-gray-50"
                  onClick={() => handleSort(col)}>
                  {label} <SortIcon column={col} />
                </th>
              ))}
              {METRIC_COLUMNS.map(({ key, shortLabel }) => (
                <th key={key} scope="col"
                  className="px-3 py-2.5 text-left text-[11px] font-semibold text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 bg-gray-50"
                  onClick={() => handleSort(key as SortColumn)}>
                  {shortLabel} <SortIcon column={key as SortColumn} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-100">
            {sortedEvaluations.map((evaluation) => (
              <tr key={evaluation.eval_run_id} className="hover:bg-blue-50/40 transition-colors">
                <td className="px-3 py-2.5 whitespace-nowrap text-sm font-medium text-gray-900">
                  {evaluation.strategy}
                </td>
                <td className="px-3 py-2.5 whitespace-nowrap text-sm">
                  <span className={`px-2 py-0.5 text-[11px] font-medium rounded-full ${
                    evaluation.query_type === 'ANN' ? 'bg-emerald-100 text-emerald-700'
                    : evaluation.query_type === 'HYBRID' ? 'bg-violet-100 text-violet-700'
                    : 'bg-sky-100 text-sky-700'
                  }`}>
                    {evaluation.query_type}
                  </span>
                </td>
                <td className="px-3 py-2.5 whitespace-nowrap text-xs font-mono text-gray-500">
                  {evaluation.build_run_id.substring(0, 8)}
                </td>
                {METRIC_COLUMNS.map(({ key }) => {
                  const val = evaluation.metrics[key] as number || 0
                  const best = isBest(key, evaluation.metrics[key] as number)
                  return (
                    <td key={key} className="px-3 py-2.5 whitespace-nowrap text-sm">
                      <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${getMetricColor(val, key)} ${best ? 'ring-1 ring-current font-bold' : ''}`}>
                        {formatMetricValue(val, key)}
                      </span>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-3 pt-3 border-t border-gray-200 flex items-center justify-between text-sm text-gray-500">
        <span>
          {filteredEvaluations.length === evaluationMetrics.length
            ? `${sortedEvaluations.length} evaluation${sortedEvaluations.length !== 1 ? 's' : ''}`
            : `${filteredEvaluations.length} of ${evaluationMetrics.length} evaluations (filtered)`
          }
        </span>
        <span className="text-xs text-gray-400">Best values per column are highlighted</span>
      </div>
    </Card>
  )
}
