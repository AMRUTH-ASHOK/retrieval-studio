import { useState, useEffect } from 'react'
import { PlayCircle } from 'lucide-react'
import { evaluationsApi } from '../services/evaluations'
import { buildsApi } from '../services/builds'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { BuildJob } from '../types'

export default function Evaluate() {
  const { selectedProject, selectedProjectId } = useProject()
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [selectedRun, setSelectedRun] = useState('')
  const [queriesTable, setQueriesTable] = useState('')
  const [topK, setTopK] = useState('10')
  const [hasGroundTruth, setHasGroundTruth] = useState(false)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    if (selectedProjectId) {
      loadBuilds()
    }
  }, [selectedProjectId])

  const loadBuilds = async () => {
    setIsLoading(true)
    try {
      const buildsData = await buildsApi.list()
      const projectBuilds = buildsData.filter(
        (b: BuildJob) => b.project_id === selectedProjectId
      )
      setBuilds(projectBuilds)
    } catch (error) {
      console.error('Failed to load builds:', error)
      setError('Failed to load builds')
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = async () => {
    if (!selectedRun || !queriesTable) {
      setError('Please fill in all required fields')
      return
    }

    setIsSubmitting(true)
    setError('')

    try {
      await evaluationsApi.create({
        run_id: selectedRun,
        queries_table: queriesTable,
      })

      alert('Evaluation job submitted successfully!')
      setSelectedRun('')
      setQueriesTable('')
    } catch (error) {
      console.error('Failed to submit evaluation:', error)
      setError('Failed to submit evaluation job')
    } finally {
      setIsSubmitting(false)
    }
  }

  const getStateBadge = (state: string) => {
    const stateMap: Record<string, 'success' | 'warning' | 'error' | 'info'> = {
      SUCCESS: 'success',
      RUNNING: 'info',
      PENDING: 'warning',
      FAILED: 'error',
    }
    return <Badge variant={stateMap[state] || 'default'}>{state}</Badge>
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Evaluate Pipeline</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">
          Submit evaluation jobs to test your retrieval pipeline performance
        </p>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to start evaluation.
          </p>
        </div>
      )}

      {selectedProject && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
          <p className="text-sm text-blue-900">
            <span className="font-medium">Current project:</span> {selectedProject.project_name}
          </p>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Submit Evaluation Form */}
        <Card>
          <h2 className="text-lg font-semibold text-databricks-gray-900 mb-6">
            Submit Evaluation
          </h2>

          <div className="space-y-4">
            <Select
              label="Build Run"
              value={selectedRun}
              onChange={(e) => setSelectedRun(e.target.value)}
              options={[
                { value: '', label: 'Select a build run' },
                ...builds.map((build) => ({
                  value: build.run_id,
                  label: `${build.run_id.substring(0, 8)} - ${build.state}`,
                })),
              ]}
              required
              helperText="Select the build run to evaluate"
            />

            <Input
              label="Queries Table"
              value={queriesTable}
              onChange={(e) => setQueriesTable(e.target.value)}
              placeholder="e.g., catalog.schema.queries_table"
              required
              helperText="Table must have 'query_text' column. Optional: 'expected_chunks' for labeled evaluation"
            />

            <Input
              label="Top K Results"
              type="number"
              value={topK}
              onChange={(e) => setTopK(e.target.value)}
              placeholder="10"
              required
              helperText="Number of top results to retrieve for evaluation (default: 10)"
            />

            <div className="flex items-center space-x-2 p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200">
              <input
                type="checkbox"
                id="hasGroundTruth"
                checked={hasGroundTruth}
                onChange={(e) => setHasGroundTruth(e.target.checked)}
                className="h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue"
              />
              <label htmlFor="hasGroundTruth" className="text-sm text-databricks-gray-700">
                My queries table includes ground truth (expected_chunks column)
              </label>
            </div>

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-md">
                <p className="text-sm text-red-800">{error}</p>
              </div>
            )}

            <Button
              variant="primary"
              onClick={handleSubmit}
              isLoading={isSubmitting}
              disabled={!selectedProjectId || !selectedRun || !queriesTable || isSubmitting}
              icon={<PlayCircle className="w-4 h-4" />}
              className="w-full"
            >
              Submit Evaluation Job
            </Button>
          </div>
        </Card>

        {/* Available Builds */}
        <Card>
          <h2 className="text-lg font-semibold text-databricks-gray-900 mb-6">
            Available Build Runs
          </h2>

          {isLoading ? (
            <div className="flex justify-center items-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
            </div>
          ) : builds.length === 0 ? (
            <div className="text-center py-8">
              <p className="text-sm text-databricks-gray-600">
                No build runs available. Create a build first.
              </p>
            </div>
          ) : (
            <div className="space-y-3 max-h-[400px] overflow-y-auto custom-scrollbar">
              {builds.map((build) => (
                <div
                  key={build.run_id}
                  className={`p-4 border rounded-md cursor-pointer transition-all ${
                    selectedRun === build.run_id
                      ? 'border-databricks-blue bg-blue-50'
                      : 'border-databricks-gray-200 hover:border-databricks-gray-300'
                  }`}
                  onClick={() => setSelectedRun(build.run_id)}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <p className="text-sm font-medium text-databricks-gray-900 font-mono">
                        {build.run_id.substring(0, 12)}...
                      </p>
                      <p className="text-xs text-databricks-gray-600 mt-1">
                        {new Date(build.created_at).toLocaleString()}
                      </p>
                    </div>
                    {getStateBadge(build.state)}
                  </div>
                  {build.config?.data_type && (
                    <p className="text-xs text-databricks-gray-600 mt-2">
                      Data Type: {build.config.data_type}
                    </p>
                  )}
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>

      {/* Info Box */}
      <Card className="mt-6 bg-databricks-gray-50">
        <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
          Evaluation Requirements
        </h3>
        <ul className="space-y-2 text-sm text-databricks-gray-700">
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              <strong>Required:</strong> Your queries table must contain a 'query_text' column
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              <strong>Labeled Evaluation:</strong> Include 'expected_chunks' column (array of chunk IDs) for ground truth metrics
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              <strong>Judge-Based Evaluation:</strong> Without ground truth, evaluation uses LLM judge scoring
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>
              Metrics computed: Recall@{topK || '10'}, NDCG@{topK || '10'}, and retrieval latency
            </span>
          </li>
          <li className="flex items-start">
            <span className="mr-2">•</span>
            <span>Results will be logged to MLflow and viewable in the Review section</span>
          </li>
        </ul>
      </Card>
    </div>
  )
}
