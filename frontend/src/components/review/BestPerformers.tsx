import { Trophy, Zap, Target, Award } from 'lucide-react'
import { Card } from '../ui/Card'
import { BestPerformer } from '../../utils/metricsAggregation'

interface BestPerformersProps {
  bestBuild: BestPerformer | null
  bestStrategy: BestPerformer | null
  fastest: BestPerformer | null
  bestOverall: BestPerformer | null
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
      icon: Trophy,
      data: bestBuild,
      color: 'blue',
      bgColor: 'bg-blue-50',
      iconColor: 'text-blue-600',
      borderColor: 'border-blue-200'
    },
    {
      title: 'Best Strategy',
      icon: Target,
      data: bestStrategy,
      color: 'green',
      bgColor: 'bg-green-50',
      iconColor: 'text-green-600',
      borderColor: 'border-green-200'
    },
    {
      title: 'Fastest',
      icon: Zap,
      data: fastest,
      color: 'yellow',
      bgColor: 'bg-yellow-50',
      iconColor: 'text-yellow-600',
      borderColor: 'border-yellow-200'
    },
    {
      title: 'Best Overall',
      icon: Award,
      data: bestOverall,
      color: 'purple',
      bgColor: 'bg-purple-50',
      iconColor: 'text-purple-600',
      borderColor: 'border-purple-200'
    }
  ]

  return (
    <Card className="mb-6">
      <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
        🏆 Best Performers
      </h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {performers.map((performer, idx) => {
          const Icon = performer.icon
          const { data } = performer

          return (
            <div
              key={idx}
              className={`p-4 rounded-lg border-2 ${performer.bgColor} ${performer.borderColor} transition-all hover:shadow-md`}
            >
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium text-databricks-gray-700">
                  {performer.title}
                </h3>
                <Icon className={`w-5 h-5 ${performer.iconColor}`} />
              </div>

              {data ? (
                <>
                  <p className="text-lg font-bold text-databricks-gray-900 mb-1 truncate" title={data.name}>
                    {data.name}
                  </p>
                  <div className="flex items-baseline gap-2">
                    <span className={`text-2xl font-bold ${performer.iconColor}`}>
                      {performer.title === 'Fastest'
                        ? `${Math.round(data.metric_value)}ms`
                        : data.metric_value.toFixed(2)
                      }
                    </span>
                    <span className="text-xs text-databricks-gray-600">
                      {data.metric_name}
                    </span>
                  </div>
                </>
              ) : (
                <div className="text-center py-4">
                  <p className="text-sm text-databricks-gray-500">No data available</p>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </Card>
  )
}
