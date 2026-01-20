import { useState } from 'react'
import Plot from 'react-plotly.js'
import { Card } from '../ui/Card'
import {
  BuildMetrics,
  StrategyMetrics,
  EvaluationMetrics,
  AggregatedMetrics,
  formatForBarChart
} from '../../utils/metricsAggregation'

interface MetricsBarChartsProps {
  buildMetrics: BuildMetrics[]
  strategyMetrics: StrategyMetrics[]
  evaluationMetrics: EvaluationMetrics[]
}

type ViewMode = 'build' | 'strategy' | 'evaluation'

export default function MetricsBarCharts({
  buildMetrics,
  strategyMetrics,
  evaluationMetrics
}: MetricsBarChartsProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('build')

  const metrics: Array<{ key: keyof AggregatedMetrics; title: string }> = [
    { key: 'recall_at_5', title: 'Recall@5' },
    { key: 'recall_at_10', title: 'Recall@10' },
    { key: 'ndcg_at_5', title: 'NDCG@5' },
    { key: 'ndcg_at_10', title: 'NDCG@10' },
    { key: 'avg_latency_ms', title: 'Latency (ms)' }
  ]

  const getDataForMode = () => {
    switch (viewMode) {
      case 'build':
        return buildMetrics
      case 'strategy':
        return strategyMetrics
      case 'evaluation':
        return evaluationMetrics
      default:
        return buildMetrics
    }
  }

  const renderChart = (metricKey: keyof AggregatedMetrics, title: string) => {
    const data = getDataForMode()
    const chartData = formatForBarChart(data, viewMode, metricKey)

    // Color scheme based on metric type
    const colors = metricKey === 'avg_latency_ms'
      ? chartData.y.map(val => {
          if (val < 100) return '#10b981' // green
          if (val < 500) return '#f59e0b' // yellow
          return '#ef4444' // red
        })
      : chartData.y.map(val => {
          if (val >= 0.8) return '#10b981'
          if (val >= 0.5) return '#f59e0b'
          return '#ef4444'
        })

    return (
      <Plot
        data={[
          {
            x: chartData.x,
            y: chartData.y,
            type: 'bar',
            marker: {
              color: colors,
            },
            text: chartData.text,
            textposition: 'outside',
            hovertemplate: '<b>%{x}</b><br>%{text}<extra></extra>',
          },
        ]}
        layout={{
          title: {
            text: title,
            font: { size: 14, weight: 600 }
          },
          xaxis: {
            title: '',
            tickangle: chartData.x.length > 5 ? -45 : 0,
          },
          yaxis: {
            title: metricKey === 'avg_latency_ms' ? 'Milliseconds' : 'Score',
          },
          height: 300,
          margin: { t: 50, b: chartData.x.length > 5 ? 100 : 60, l: 60, r: 20 },
          showlegend: false,
        }}
        config={{ responsive: true, displayModeBar: false }}
        style={{ width: '100%' }}
        className="plotly-chart"
      />
    )
  }

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-databricks-gray-900">
          📊 Metrics Comparison
        </h2>

        {/* View mode toggle */}
        <div className="flex gap-2">
          <button
            onClick={() => setViewMode('build')}
            className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              viewMode === 'build'
                ? 'bg-databricks-blue text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            By Builds
          </button>
          <button
            onClick={() => setViewMode('strategy')}
            className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              viewMode === 'strategy'
                ? 'bg-databricks-blue text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            By Strategies
          </button>
          <button
            onClick={() => setViewMode('evaluation')}
            className={`px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              viewMode === 'evaluation'
                ? 'bg-databricks-blue text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            By Evaluations
          </button>
        </div>
      </div>

      {getDataForMode().length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">
            No data available for this view.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {metrics.map(({ key, title }) => (
            <div key={key} className="bg-white">
              {renderChart(key, title)}
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}
