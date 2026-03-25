import { Trophy, Zap, Target, Award } from 'lucide-react'
import { Card } from '../ui/Card'
import { BestPerformer } from '../../utils/metricsAggregation'

interface BestPerformersProps {
  bestBuild: BestPerformer | null
  bestStrategy: BestPerformer | null
  fastest: BestPerformer | null
  bestOverall: BestPerformer | null
}

function ScoreRing({ value, max, color }: { value: number; max: number; color: string }) {
  const pct = Math.min(100, (value / max) * 100)
  const r = 22
  const circ = 2 * Math.PI * r
  const offset = circ - (pct / 100) * circ

  return (
    <svg width="54" height="54" className="flex-shrink-0">
      <circle cx="27" cy="27" r={r} fill="none" stroke="#e5e7eb" strokeWidth="4" />
      <circle
        cx="27" cy="27" r={r} fill="none"
        stroke={color} strokeWidth="4"
        strokeDasharray={circ} strokeDashoffset={offset}
        strokeLinecap="round"
        transform="rotate(-90 27 27)"
        className="transition-all duration-700"
      />
      <text x="27" y="27" textAnchor="middle" dominantBaseline="central"
        fill="#111827" fontSize="11" fontWeight="700">
        {Math.round(pct)}%
      </text>
    </svg>
  )
}

export default function BestPerformers({
  bestBuild,
  bestStrategy,
  fastest,
  bestOverall
}: BestPerformersProps) {
  const performers = [
    {
      title: 'Best Build',
      subtitle: 'Highest Recall@10',
      icon: Trophy,
      data: bestBuild,
      ringMax: 1,
      accentHex: '#2563eb',
      bgColor: 'bg-blue-50',
      iconColor: 'text-blue-600',
      borderColor: 'border-blue-200',
      gradientFrom: 'from-blue-500/5',
    },
    {
      title: 'Best Strategy',
      subtitle: 'Highest NDCG@5',
      icon: Target,
      data: bestStrategy,
      ringMax: 1,
      accentHex: '#059669',
      bgColor: 'bg-green-50',
      iconColor: 'text-green-600',
      borderColor: 'border-green-200',
      gradientFrom: 'from-green-500/5',
    },
    {
      title: 'Fastest',
      subtitle: 'Lowest Latency',
      icon: Zap,
      data: fastest,
      ringMax: 1000,
      accentHex: '#d97706',
      bgColor: 'bg-amber-50',
      iconColor: 'text-amber-600',
      borderColor: 'border-amber-200',
      gradientFrom: 'from-amber-500/5',
    },
    {
      title: 'Best Overall',
      subtitle: '40% Recall + 40% NDCG + 20% Speed',
      icon: Award,
      data: bestOverall,
      ringMax: 10,
      accentHex: '#7c3aed',
      bgColor: 'bg-purple-50',
      iconColor: 'text-purple-600',
      borderColor: 'border-purple-200',
      gradientFrom: 'from-purple-500/5',
    }
  ]

  return (
    <Card className="mb-6">
      <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
        Best Performers
      </h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {performers.map((performer, idx) => {
          const Icon = performer.icon
          const { data } = performer

          const ringVal = data
            ? performer.title === 'Fastest'
              ? Math.max(0, performer.ringMax - data.metric_value)
              : data.metric_value
            : 0

          return (
            <div
              key={idx}
              className={`relative p-4 rounded-xl border ${performer.borderColor} bg-gradient-to-br ${performer.gradientFrom} to-white transition-all hover:shadow-lg hover:-translate-y-0.5`}
            >
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h3 className="text-sm font-semibold text-databricks-gray-800">
                    {performer.title}
                  </h3>
                  <p className="text-[10px] text-databricks-gray-400 mt-0.5">
                    {performer.subtitle}
                  </p>
                </div>
                <div className={`w-8 h-8 rounded-lg ${performer.bgColor} flex items-center justify-center`}>
                  <Icon className={`w-4 h-4 ${performer.iconColor}`} />
                </div>
              </div>

              {data ? (
                <div className="flex items-center gap-3">
                  <ScoreRing value={ringVal} max={performer.ringMax} color={performer.accentHex} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-bold text-databricks-gray-900 truncate" title={data.name}>
                      {data.name}
                    </p>
                    <div className="flex items-baseline gap-1.5 mt-0.5">
                      <span className={`text-xl font-bold ${performer.iconColor}`}>
                        {performer.title === 'Fastest'
                          ? `${Math.round(data.metric_value)}ms`
                          : data.metric_value.toFixed(3)
                        }
                      </span>
                    </div>
                    <span className="text-[10px] text-databricks-gray-500">
                      {data.metric_name}
                    </span>
                  </div>
                </div>
              ) : (
                <div className="text-center py-4">
                  <p className="text-sm text-databricks-gray-400">No data</p>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </Card>
  )
}
