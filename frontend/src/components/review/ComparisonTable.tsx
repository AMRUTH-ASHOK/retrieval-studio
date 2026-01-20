import { useState } from 'react'
import { ArrowUpDown, Download } from 'lucide-react'
import { Card } from '../ui/Card'
import { Button } from '../ui/Button'
import { EvaluationMetrics, formatMetricValue, getMetricColor } from '../../utils/metricsAggregation'

interface ComparisonTableProps {
  evaluationMetrics: EvaluationMetrics[]
}

type SortColumn = 'eval_name' | 'build_run_id' | 'strategy' | 'query_type' |
  'recall_at_5' | 'recall_at_10' | 'ndcg_at_5' | 'ndcg_at_10' |
  'avg_relevance_at_5' | 'avg_latency_ms' | 'num_queries'

export default function ComparisonTable({ evaluationMetrics }: ComparisonTableProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>('recall_at_10')
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc')

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc')
    } else {
      setSortColumn(column)
      setSortDirection('desc')
    }
  }

  const sortedEvaluations = [...evaluationMetrics].sort((a, b) => {
    let aVal: any
    let bVal: any

    if (sortColumn === 'eval_name' || sortColumn === 'build_run_id' ||
        sortColumn === 'strategy' || sortColumn === 'query_type') {
      aVal = a[sortColumn]
      bVal = b[sortColumn]
    } else {
      aVal = a.metrics[sortColumn] || 0
      bVal = b.metrics[sortColumn] || 0
    }

    if (typeof aVal === 'string') {
      return sortDirection === 'asc'
        ? aVal.localeCompare(bVal)
        : bVal.localeCompare(aVal)
    } else {
      return sortDirection === 'asc' ? aVal - bVal : bVal - aVal
    }
  })

  const exportToCSV = () => {
    const headers = [
      'Evaluation ID',
      'Build ID',
      'Strategy',
      'Query Type',
      'Recall@5',
      'Recall@10',
      'NDCG@5',
      'NDCG@10',
      'Avg Relevance@5',
      'Latency (ms)',
      'Num Queries'
    ]

    const rows = sortedEvaluations.map(evaluation => [
      `"${evaluation.eval_run_id}"`,
      `"${evaluation.build_run_id}"`,
      `"${evaluation.strategy}"`,
      `"${evaluation.query_type}"`,
      evaluation.metrics.recall_at_5 || '',
      evaluation.metrics.recall_at_10 || '',
      evaluation.metrics.ndcg_at_5 || '',
      evaluation.metrics.ndcg_at_10 || '',
      evaluation.metrics.avg_relevance_at_5 || '',
      evaluation.metrics.avg_latency_ms || '',
      evaluation.metrics.num_queries || ''
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

  const SortIcon = ({ column }: { column: SortColumn }) => (
    <ArrowUpDown
      className={`w-4 h-4 inline ml-1 ${
        sortColumn === column ? 'text-databricks-blue' : 'text-gray-400'
      }`}
    />
  )

  if (evaluationMetrics.length === 0) {
    return (
      <Card className="mb-6">
        <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
          📋 Detailed Comparison Table
        </h2>
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">
            No evaluation data to display.
          </p>
        </div>
      </Card>
    )
  }

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-databricks-gray-900">
          📋 Detailed Comparison Table
        </h2>
        <Button
          variant="outline"
          size="sm"
          onClick={exportToCSV}
          icon={<Download className="w-4 h-4" />}
        >
          Export CSV
        </Button>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('eval_name')}
              >
                Evaluation ID <SortIcon column="eval_name" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('build_run_id')}
              >
                Build ID <SortIcon column="build_run_id" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('strategy')}
              >
                Strategy <SortIcon column="strategy" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('query_type')}
              >
                Query Type <SortIcon column="query_type" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('recall_at_5')}
              >
                Recall@5 <SortIcon column="recall_at_5" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('recall_at_10')}
              >
                Recall@10 <SortIcon column="recall_at_10" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('ndcg_at_5')}
              >
                NDCG@5 <SortIcon column="ndcg_at_5" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('ndcg_at_10')}
              >
                NDCG@10 <SortIcon column="ndcg_at_10" />
              </th>
              <th
                scope="col"
                className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => handleSort('avg_latency_ms')}
              >
                Latency <SortIcon column="avg_latency_ms" />
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedEvaluations.map((evaluation) => (
              <tr key={evaluation.eval_run_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 whitespace-nowrap text-sm font-mono text-gray-900">
                  {evaluation.eval_run_id.substring(0, 12)}...
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm font-mono text-gray-700">
                  {evaluation.build_run_id.substring(0, 8)}...
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                  {evaluation.strategy}
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 text-xs font-medium rounded ${
                    evaluation.query_type === 'ANN'
                      ? 'bg-green-100 text-green-800'
                      : 'bg-blue-100 text-blue-800'
                  }`}>
                    {evaluation.query_type}
                  </span>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    getMetricColor(evaluation.metrics.recall_at_5 || 0, 'recall_at_5')
                  }`}>
                    {formatMetricValue(evaluation.metrics.recall_at_5 || 0, 'recall_at_5')}
                  </span>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    getMetricColor(evaluation.metrics.recall_at_10 || 0, 'recall_at_10')
                  }`}>
                    {formatMetricValue(evaluation.metrics.recall_at_10 || 0, 'recall_at_10')}
                  </span>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    getMetricColor(evaluation.metrics.ndcg_at_5 || 0, 'ndcg_at_5')
                  }`}>
                    {formatMetricValue(evaluation.metrics.ndcg_at_5 || 0, 'ndcg_at_5')}
                  </span>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    getMetricColor(evaluation.metrics.ndcg_at_10 || 0, 'ndcg_at_10')
                  }`}>
                    {formatMetricValue(evaluation.metrics.ndcg_at_10 || 0, 'ndcg_at_10')}
                  </span>
                </td>
                <td className="px-4 py-3 whitespace-nowrap text-sm">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${
                    getMetricColor(evaluation.metrics.avg_latency_ms || 0, 'avg_latency_ms')
                  }`}>
                    {formatMetricValue(evaluation.metrics.avg_latency_ms || 0, 'avg_latency_ms')}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-4 pt-4 border-t border-gray-200 text-sm text-gray-600">
        Showing {sortedEvaluations.length} evaluation{sortedEvaluations.length !== 1 ? 's' : ''}
      </div>
    </Card>
  )
}
