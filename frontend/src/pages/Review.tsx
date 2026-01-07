import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '../components/ui/Table'
import { BuildJob } from '../types'

export default function Review() {
  const { selectedProject, selectedProjectId } = useProject()
  const [builds, setBuilds] = useState<BuildJob[]>([])
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
    } finally {
      setIsLoading(false)
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
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-databricks-gray-900">Review Results</h1>
          <p className="text-sm text-databricks-gray-600 mt-1">
            View build and evaluation job results
          </p>
        </div>
        <Button
          variant="outline"
          onClick={loadBuilds}
          icon={<RefreshCw className="w-4 h-4" />}
          disabled={isLoading || !selectedProjectId}
        >
          Refresh
        </Button>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to view results.
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

      {isLoading ? (
        <Card>
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
          </div>
        </Card>
      ) : builds.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-databricks-gray-100 rounded-full flex items-center justify-center">
              <ExternalLink className="w-8 h-8 text-databricks-gray-400" />
            </div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">
              No build runs yet
            </h3>
            <p className="text-sm text-databricks-gray-600">
              Create a build to see results here
            </p>
          </div>
        </Card>
      ) : (
        <Card padding={false}>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Run ID</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Data Type</TableHead>
                <TableHead>Strategies</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {builds.map((build) => (
                <TableRow key={build.run_id}>
                  <TableCell>
                    <code className="text-xs font-mono bg-databricks-gray-100 px-2 py-1 rounded">
                      {build.run_id.substring(0, 12)}...
                    </code>
                  </TableCell>
                  <TableCell>{getStateBadge(build.state)}</TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-700">
                      {build.config?.data_type || '-'}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-700">
                      {build.config?.strategies
                        ? Object.keys(build.config.strategies).length
                        : 0}{' '}
                      strategies
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-600">
                      {new Date(build.created_at).toLocaleString()}
                    </span>
                  </TableCell>
                  <TableCell>
                    {build.job_id && (
                      <Button
                        variant="ghost"
                        size="sm"
                        icon={<ExternalLink className="w-3 h-3" />}
                        onClick={() => {
                          // In a real implementation, this would link to the Databricks job
                          alert(`View job: ${build.job_id}`)
                        }}
                      >
                        View Job
                      </Button>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}

      {builds.length > 0 && (
        <Card className="mt-6 bg-databricks-gray-50">
          <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
            Viewing Results
          </h3>
          <ul className="space-y-2 text-sm text-databricks-gray-700">
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                Build results are logged to MLflow experiments in your Databricks workspace
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                Navigate to the MLflow UI in Databricks to view detailed metrics and artifacts
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                Use the Leaderboard page to compare performance across different strategies
              </span>
            </li>
          </ul>
        </Card>
      )}
    </div>
  )
}
