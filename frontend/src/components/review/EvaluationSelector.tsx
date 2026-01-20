import { Check, ChevronDown, ChevronRight } from 'lucide-react'
import { useState } from 'react'
import { Card } from '../ui/Card'
import { Evaluation } from '../../types'

interface EvaluationSelectorProps {
  evaluationsByBuild: Map<string, Evaluation[]>
  selectedEvalIds: Set<string>
  onToggleEvaluation: (evalId: string) => void
  isLoading?: boolean
}

export default function EvaluationSelector({
  evaluationsByBuild,
  selectedEvalIds,
  onToggleEvaluation,
  isLoading = false
}: EvaluationSelectorProps) {
  // Track which builds are expanded
  const [expandedBuilds, setExpandedBuilds] = useState<Set<string>>(
    new Set(Array.from(evaluationsByBuild.keys()))
  )

  const toggleBuildExpansion = (buildId: string) => {
    setExpandedBuilds(prev => {
      const newSet = new Set(prev)
      if (newSet.has(buildId)) {
        newSet.delete(buildId)
      } else {
        newSet.add(buildId)
      }
      return newSet
    })
  }

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
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

  const totalEvaluations = Array.from(evaluationsByBuild.values()).reduce(
    (sum, evals) => sum + evals.filter(e => e.state === 'SUCCESS').length,
    0
  )

  return (
    <Card className="mb-6">
      <h2 className="text-lg font-semibold text-databricks-gray-900 mb-2">
        Step 2: Select Evaluations
      </h2>
      <p className="text-sm text-databricks-gray-600 mb-4">
        Choose evaluation runs from the selected builds
      </p>

      {isLoading ? (
        <div className="flex justify-center items-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
        </div>
      ) : evaluationsByBuild.size === 0 ? (
        <div className="text-center py-12 bg-gray-50 rounded-lg border border-dashed border-gray-300">
          <p className="text-databricks-gray-600">
            {evaluationsByBuild.size === 0
              ? 'Select builds above to see their evaluations'
              : 'No evaluations found for selected builds'
            }
          </p>
          <p className="text-xs text-databricks-gray-500 mt-1">
            Run an evaluation job for your builds first.
          </p>
        </div>
      ) : (
        <div className="space-y-3 max-h-96 overflow-y-auto">
          {Array.from(evaluationsByBuild.entries()).map(([buildId, evaluations]) => {
            const successfulEvals = evaluations.filter(e => e.state === 'SUCCESS')
            const isExpanded = expandedBuilds.has(buildId)

            if (successfulEvals.length === 0) return null

            return (
              <div key={buildId} className="border border-databricks-gray-200 rounded-lg">
                {/* Build header */}
                <button
                  onClick={() => toggleBuildExpansion(buildId)}
                  className="w-full p-3 flex items-center justify-between hover:bg-databricks-gray-50 transition-colors rounded-t-lg"
                >
                  <div className="flex items-center gap-2">
                    {isExpanded ? (
                      <ChevronDown className="w-4 h-4 text-databricks-gray-600" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-databricks-gray-600" />
                    )}
                    <span className="font-mono text-sm font-semibold text-databricks-gray-900">
                      Build {buildId.substring(0, 8)}...
                    </span>
                    <span className="text-xs text-databricks-gray-600">
                      ({successfulEvals.length} {successfulEvals.length === 1 ? 'evaluation' : 'evaluations'})
                    </span>
                  </div>
                </button>

                {/* Evaluations list */}
                {isExpanded && (
                  <div className="p-3 pt-0 space-y-2">
                    {successfulEvals.map((evaluation) => {
                      const isSelected = selectedEvalIds.has(evaluation.run_id)

                      return (
                        <div
                          key={evaluation.run_id}
                          onClick={() => onToggleEvaluation(evaluation.run_id)}
                          className={`p-3 border-2 rounded-md cursor-pointer transition-all ${
                            isSelected
                              ? 'border-databricks-blue bg-blue-50'
                              : 'border-databricks-gray-200 hover:border-databricks-gray-300 hover:bg-databricks-gray-50'
                          }`}
                        >
                          <div className="flex items-start gap-3">
                            {/* Checkbox */}
                            <div className={`w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0 mt-0.5 ${
                              isSelected
                                ? 'bg-databricks-blue border-databricks-blue'
                                : 'border-databricks-gray-400'
                            }`}>
                              {isSelected && <Check className="w-3 h-3 text-white" />}
                            </div>

                            {/* Evaluation info */}
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between mb-1">
                                <code className="text-xs font-mono text-databricks-gray-900">
                                  {evaluation.run_id.substring(0, 12)}...
                                </code>
                                {getStatusBadge(evaluation.state)}
                              </div>

                              <div className="text-xs text-databricks-gray-600">
                                📅 {formatDate(evaluation.created_at)}
                              </div>
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}

      {totalEvaluations > 0 && (
        <div className="mt-4 pt-4 border-t border-databricks-gray-200">
          <p className="text-sm text-databricks-gray-600">
            <span className="font-semibold">{selectedEvalIds.size}</span> of{' '}
            <span className="font-semibold">{totalEvaluations}</span> evaluations selected
          </p>
        </div>
      )}
    </Card>
  )
}
