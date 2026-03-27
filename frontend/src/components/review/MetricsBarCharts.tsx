import { useState } from 'react'
import Plot from 'react-plotly.js'
import { Card } from '../ui/Card'
import {
  BuildMetrics,
  StrategyMetrics,
  EvaluationMetrics,
  AggregatedMetrics,
  formatForBarChart,
  formatMetricValue
} from '../../utils/metricsAggregation'

interface MetricsBarChartsProps {
  buildMetrics: BuildMetrics[]
  strategyMetrics: StrategyMetrics[]
  evaluationMetrics: EvaluationMetrics[]
}

type ViewMode = 'build' | 'strategy' | 'evaluation'
type ChartType = 'bar' | 'scatter'

const METRIC_DESCRIPTIONS: Record<string, string> = {
  precision_at_5: 'Fraction of top-5 retrieved chunks that are relevant',
  precision_at_10: 'Fraction of top-10 retrieved chunks that are relevant',
  recall_at_5: 'Fraction of relevant chunks found in top-5 results',
  recall_at_10: 'Fraction of relevant chunks found in top-10 results',
  ndcg_at_5: 'Ranking quality of top-5 results (higher = better order)',
  ndcg_at_10: 'Ranking quality of top-10 results (higher = better order)',
  avg_latency_ms: 'Average query response time in milliseconds',
}

const COLOR_SCALE = {
  good: '#059669',
  mid: '#d97706',
  poor: '#dc2626',
  gradient: (val: number, isLatency: boolean) => {
    if (isLatency) {
      if (val < 100) return '#059669'
      if (val < 300) return '#10b981'
      if (val < 500) return '#d97706'
      if (val < 800) return '#ea580c'
      return '#dc2626'
    }
    if (val >= 0.85) return '#059669'
    if (val >= 0.7) return '#10b981'
    if (val >= 0.5) return '#d97706'
    if (val >= 0.3) return '#ea580c'
    return '#dc2626'
  }
}

export default function MetricsBarCharts({
  buildMetrics,
  strategyMetrics,
  evaluationMetrics
}: MetricsBarChartsProps) {
  const [viewMode, setViewMode] = useState<ViewMode>('build')
  const [chartType, setChartType] = useState<ChartType>('bar')

  const metrics: Array<{ key: keyof AggregatedMetrics; title: string }> = [
    { key: 'precision_at_5', title: 'Precision@5' },
    { key: 'precision_at_10', title: 'Precision@10' },
    { key: 'recall_at_5', title: 'Recall@5' },
    { key: 'recall_at_10', title: 'Recall@10' },
    { key: 'ndcg_at_5', title: 'NDCG@5' },
    { key: 'ndcg_at_10', title: 'NDCG@10' },
    { key: 'avg_latency_ms', title: 'Latency (ms)' }
  ]

  const getDataForMode = () => {
    switch (viewMode) {
      case 'build': return buildMetrics
      case 'strategy': return strategyMetrics
      case 'evaluation': return evaluationMetrics
      default: return buildMetrics
    }
  }

  const renderBarChart = (metricKey: keyof AggregatedMetrics, title: string) => {
    const data = getDataForMode()
    const chartData = formatForBarChart(data, viewMode, metricKey)
    const isLatency = metricKey === 'avg_latency_ms'

    const bestVal = isLatency
      ? Math.min(...chartData.y.filter(v => v > 0))
      : Math.max(...chartData.y)

    const colors = chartData.y.map(val => COLOR_SCALE.gradient(val, isLatency))

    const hoverTexts = chartData.y.map((val, i) => {
      const rank = [...chartData.y]
        .sort((a, b) => isLatency ? a - b : b - a)
        .indexOf(val) + 1
      const delta = isLatency ? val - bestVal : bestVal - val
      const deltaStr = delta === 0
        ? 'Best'
        : isLatency
          ? `+${Math.round(delta)}ms vs best`
          : `-${delta.toFixed(3)} vs best`
      return `<b>${chartData.x[i]}</b><br>${title}: <b>${formatMetricValue(val, metricKey)}</b><br>Rank: #${rank}<br>${deltaStr}`
    })

    return (
      <Plot
        data={[{
          x: chartData.x,
          y: chartData.y,
          type: 'bar',
          marker: {
            color: colors,
            line: { color: colors.map(c => c + '40'), width: 1 }
          },
          text: chartData.text,
          textposition: 'outside',
          cliponaxis: false,
          textfont: { size: 11, color: '#374151' },
          hovertemplate: hoverTexts.map(t => t + '<extra></extra>'),
        }]}
        layout={{
          title: {
            text: `${title} <span style="font-size:10px;color:#6b7280;font-weight:normal">${METRIC_DESCRIPTIONS[metricKey] || ''}</span>`,
            font: { size: 13, color: '#111827' },
            x: 0.01,
            xanchor: 'left',
          },
          xaxis: {
            title: '',
            tickangle: chartData.x.length > 4 ? -35 : 0,
            tickfont: { size: 11 },
          },
          yaxis: {
            title: isLatency ? 'ms' : 'Score',
            titlefont: { size: 11, color: '#6b7280' },
            gridcolor: '#f3f4f6',
            zeroline: false,
          },
          height: 340,
          margin: { t: 80, b: chartData.x.length > 4 ? 100 : 60, l: 55, r: 15 },
          showlegend: false,
          plot_bgcolor: 'white',
          paper_bgcolor: 'white',
          hoverlabel: { bgcolor: 'white', bordercolor: '#d1d5db', font: { size: 12, color: '#111827' } },
        }}
        config={{ responsive: true, displayModeBar: false }}
        style={{ width: '100%' }}
      />
    )
  }

  const renderScatterPlot = () => {
    const data = getDataForMode()
    const chartData = formatForBarChart(data, viewMode, 'avg_latency_ms')
    const recallData = formatForBarChart(data, viewMode, 'recall_at_10')

    const colors = recallData.y.map(val => COLOR_SCALE.gradient(val, false))
    const sizes = recallData.y.map(v => 10 + v * 20)

    return (
      <Plot
        data={[{
          x: chartData.y,
          y: recallData.y,
          mode: 'markers+text',
          type: 'scatter',
          marker: {
            size: sizes,
            color: colors,
            line: { color: '#fff', width: 1.5 },
            opacity: 0.85,
          },
          text: chartData.x,
          textposition: 'top center',
          textfont: { size: 10, color: '#374151' },
          hovertemplate: chartData.x.map((name, i) =>
            `<b>${name}</b><br>Latency: ${Math.round(chartData.y[i])}ms<br>Recall@10: ${recallData.y[i].toFixed(3)}<extra></extra>`
          ),
        }]}
        layout={{
          title: {
            text: 'Latency vs Recall@10 <span style="font-size:10px;color:#6b7280;font-weight:normal">Lower-left = fast but low recall, upper-left = ideal</span>',
            font: { size: 13, color: '#111827' },
            x: 0.01,
            xanchor: 'left',
          },
          xaxis: {
            title: 'Latency (ms)',
            titlefont: { size: 12, color: '#6b7280' },
            gridcolor: '#f3f4f6',
          },
          yaxis: {
            title: 'Recall@10',
            titlefont: { size: 12, color: '#6b7280' },
            gridcolor: '#f3f4f6',
            range: [0, 1.05],
          },
          height: 420,
          margin: { t: 60, b: 60, l: 60, r: 30 },
          showlegend: false,
          plot_bgcolor: 'white',
          paper_bgcolor: 'white',
          hoverlabel: { bgcolor: 'white', bordercolor: '#d1d5db', font: { size: 12, color: '#111827' } },
          shapes: [{
            type: 'line', x0: 0, x1: 1, y0: 0.8, y1: 0.8,
            xref: 'paper', yref: 'y',
            line: { color: '#059669', width: 1, dash: 'dot' },
          }],
          annotations: [{
            x: 1, y: 0.8, xref: 'paper', yref: 'y',
            text: 'Good recall threshold',
            showarrow: false, font: { size: 9, color: '#059669' },
            xanchor: 'right',
          }],
        }}
        config={{ responsive: true, displayModeBar: false }}
        style={{ width: '100%' }}
      />
    )
  }

  return (
    <Card className="mb-6">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <h2 className="text-lg font-semibold text-databricks-gray-900">
          Metrics Comparison
        </h2>

        <div className="flex items-center gap-3">
          <div className="flex gap-1 bg-gray-100 rounded-lg p-0.5">
            <button
              onClick={() => setChartType('bar')}
              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${chartType === 'bar' ? 'bg-white text-databricks-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
            >
              Bar Charts
            </button>
            <button
              onClick={() => setChartType('scatter')}
              className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${chartType === 'scatter' ? 'bg-white text-databricks-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
            >
              Scatter Plot
            </button>
          </div>

          <div className="h-5 w-px bg-gray-200" />

          <div className="flex gap-1 bg-gray-100 rounded-lg p-0.5">
            {(['build', 'strategy', 'evaluation'] as ViewMode[]).map(mode => (
              <button
                key={mode}
                onClick={() => setViewMode(mode)}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors capitalize ${viewMode === mode ? 'bg-databricks-blue text-white shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
              >
                {mode === 'build' ? 'Builds' : mode === 'strategy' ? 'Strategies' : 'Evaluations'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {getDataForMode().length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">No data available for this view.</p>
        </div>
      ) : chartType === 'scatter' ? (
        <div className="bg-white">{renderScatterPlot()}</div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {metrics.map(({ key, title }) => (
            <div key={key} className="bg-white rounded-lg border border-gray-100">
              {renderBarChart(key, title)}
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}
