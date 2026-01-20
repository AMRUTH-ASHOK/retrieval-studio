import { Check } from 'lucide-react'
import { Card } from '../ui/Card'
import { BuildJob } from '../../types'

interface BuildSelectorProps {
  builds: BuildJob[]
  selectedBuildIds: Set<string>
  onToggleBuild: (buildId: string) => void
  isLoading?: boolean
}

export default function BuildSelector({
  builds,
  selectedBuildIds,
  onToggleBuild,
  isLoading = false
}: BuildSelectorProps) {
  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  const getStrategyCount = (build: BuildJob): number => {
    if (build.config?.strategies) {
      return Object.keys(build.config.strategies).length
    }
    return 0
  }

  const getStatusBadge = (state: string) => {
    const statusMap: Record<string, string> = {
      SUCCESS: 'bg-green-100 text-green-800',
      FAILED: 'bg-red-100 text-red-800',
      RUNNING: 'bg-blue-100 text-blue-800',
      PENDING: 'bg-yellow-100 text-yellow-800'
    }

    return (
      <span className={`px-2 py-1 text-xs font-medium rounded ${statusMap[state] || 'bg-gray-100 text-gray-800'}`}>
        {state}
      </span>
    )
  }

  // Filter for successful builds only
  const successfulBuilds = builds.filter(b => b.state === 'SUCCESS')

  return (
    <Card className="mb-6">
      <h2 className="text-lg font-semibold text-databricks-gray-900 mb-2">
        Step 1: Select Builds
      </h2>
      <p className="text-sm text-databricks-gray-600 mb-4">
        Choose one or more successful builds to compare
      </p>

      {isLoading ? (
        <div className="flex justify-center items-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
        </div>
      ) : successfulBuilds.length === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">
            No successful builds found for this project.
          </p>
          <p className="text-xs text-databricks-gray-500 mt-1">
            Create and complete a build job first.
          </p>
        </div>
      ) : (
        <div className="space-y-2 max-h-96 overflow-y-auto">
          {successfulBuilds.map((build) => {
            const isSelected = selectedBuildIds.has(build.run_id)
            const strategyCount = getStrategyCount(build)

            return (
              <div
                key={build.run_id}
                onClick={() => onToggleBuild(build.run_id)}
                className={`p-4 border-2 rounded-lg cursor-pointer transition-all ${
                  isSelected
                    ? 'border-databricks-blue bg-blue-50 shadow-md'
                    : 'border-databricks-gray-200 hover:border-databricks-gray-300 hover:bg-databricks-gray-50'
                }`}
              >
                <div className="flex items-start gap-3">
                  {/* Checkbox */}
                  <div className={`w-5 h-5 rounded border-2 flex items-center justify-center flex-shrink-0 mt-0.5 ${
                    isSelected
                      ? 'bg-databricks-blue border-databricks-blue'
                      : 'border-databricks-gray-400'
                  }`}>
                    {isSelected && <Check className="w-3 h-3 text-white" />}
                  </div>

                  {/* Build info */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <code className="text-sm font-mono text-databricks-gray-900 font-semibold">
                          {build.run_id.substring(0, 8)}...
                        </code>
                        {getStatusBadge(build.state)}
                      </div>
                    </div>

                    <div className="flex items-center gap-4 text-xs text-databricks-gray-600">
                      <span>📅 {formatDate(build.created_at)}</span>
                      <span>🔧 {strategyCount} {strategyCount === 1 ? 'strategy' : 'strategies'}</span>
                    </div>

                    {build.config?.data_type && (
                      <div className="mt-2 text-xs text-databricks-gray-600">
                        <span className="font-medium">Data type:</span> {build.config.data_type}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      )}

      {successfulBuilds.length > 0 && (
        <div className="mt-4 pt-4 border-t border-databricks-gray-200">
          <p className="text-sm text-databricks-gray-600">
            <span className="font-semibold">{selectedBuildIds.size}</span> of{' '}
            <span className="font-semibold">{successfulBuilds.length}</span> builds selected
          </p>
        </div>
      )}
    </Card>
  )
}
