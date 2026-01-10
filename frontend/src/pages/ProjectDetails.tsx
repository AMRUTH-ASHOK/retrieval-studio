import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Plus, ExternalLink, Play, RefreshCw, CheckCircle, XCircle, Clock, AlertCircle } from 'lucide-react'
import { projectsApi } from '../services/projects'
import { buildsApi } from '../services/builds'
import { Button } from '../components/ui/Button'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import type { Project, BuildJob } from '../types'

export default function ProjectDetails() {
  const { projectId } = useParams<{ projectId: string }>()
  const navigate = useNavigate()
  const [project, setProject] = useState<Project | null>(null)
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    if (projectId) {
      loadProjectDetails()
    }
  }, [projectId])

  const loadProjectDetails = async () => {
    if (!projectId) return

    setIsLoading(true)
    setError('')
    try {
      // Load project info
      const projectData = await projectsApi.getById(projectId)
      setProject(projectData)

      // Load builds for this project
      const buildsData = await buildsApi.getByProject(projectId)
      // Sort by created_at descending (most recent first)
      const sortedBuilds = buildsData.sort(
        (a: BuildJob, b: BuildJob) =>
          new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      )
      setBuilds(sortedBuilds)
    } catch (err: any) {
      console.error('Failed to load project details:', err)
      setError(err?.response?.data?.detail || 'Failed to load project details')
    } finally {
      setIsLoading(false)
    }
  }

  const handleCreateBuild = () => {
    navigate('/build')
  }

  const handleEvaluate = (runId: string) => {
    navigate('/evaluate')
  }

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr)
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  const getStatusIcon = (state: string) => {
    switch (state) {
      case 'SUCCESS':
        return <CheckCircle className="w-5 h-5 text-green-600" />
      case 'FAILED':
        return <XCircle className="w-5 h-5 text-red-600" />
      case 'RUNNING':
        return <RefreshCw className="w-5 h-5 text-blue-600 animate-spin" />
      case 'PENDING':
        return <Clock className="w-5 h-5 text-yellow-600" />
      default:
        return <AlertCircle className="w-5 h-5 text-gray-600" />
    }
  }

  const getStatusBadge = (state: string) => {
    switch (state) {
      case 'SUCCESS':
        return <Badge variant="success">{state}</Badge>
      case 'FAILED':
        return <Badge variant="error">{state}</Badge>
      case 'RUNNING':
        return <Badge variant="info">{state}</Badge>
      case 'PENDING':
        return <Badge variant="warning">{state}</Badge>
      default:
        return <Badge variant="default">{state}</Badge>
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <RefreshCw className="w-8 h-8 text-databricks-blue animate-spin mx-auto mb-2" />
          <p className="text-sm text-databricks-gray-600">Loading project details...</p>
        </div>
      </div>
    )
  }

  if (error || !project) {
    return (
      <div className="p-6">
        <Button
          variant="ghost"
          onClick={() => navigate('/projects')}
          icon={<ArrowLeft className="w-4 h-4" />}
          className="mb-4"
        >
          Back to Projects
        </Button>
        <div className="p-4 bg-red-50 border border-red-200 rounded-md">
          <p className="text-sm text-red-800">{error || 'Project not found'}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <Button
          variant="ghost"
          onClick={() => navigate('/projects')}
          icon={<ArrowLeft className="w-4 h-4" />}
          className="mb-4"
        >
          Back to Projects
        </Button>

        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-databricks-gray-900">
              {project.project_name}
            </h1>
            {project.description && (
              <p className="text-sm text-databricks-gray-600 mt-1">{project.description}</p>
            )}
            <div className="flex items-center gap-4 mt-3 text-sm text-databricks-gray-500">
              <span>Created: {formatDate(project.created_at)}</span>
              {project.vs_endpoint_name && (
                <span>VS Endpoint: {project.vs_endpoint_name}</span>
              )}
            </div>
          </div>

          <Button
            variant="primary"
            onClick={handleCreateBuild}
            icon={<Plus className="w-4 h-4" />}
          >
            Create New Build
          </Button>
        </div>
      </div>

      {/* Build History */}
      <div>
        <h2 className="text-lg font-semibold text-databricks-gray-900 mb-4">
          Build History
        </h2>

        {builds.length === 0 ? (
          <Card className="bg-blue-50 border-blue-200">
            <CardContent className="p-6 text-center">
              <p className="text-sm text-blue-900 mb-4">
                No builds yet. Create your first build to get started!
              </p>
              <Button
                variant="primary"
                onClick={handleCreateBuild}
                icon={<Plus className="w-4 h-4" />}
              >
                Create First Build
              </Button>
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-4">
            {builds.map((build) => (
              <Card key={build.run_id}>
                <CardContent className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex items-start gap-3">
                      {getStatusIcon(build.state)}
                      <div>
                        <div className="flex items-center gap-2 mb-1">
                          <h3 className="font-semibold text-databricks-gray-900">
                            Build #{build.run_id.substring(0, 8)}
                          </h3>
                          {getStatusBadge(build.state)}
                        </div>
                        <p className="text-sm text-databricks-gray-600 font-mono">
                          Run ID: {build.run_id}
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-4 mb-4 text-sm">
                    <div>
                      <span className="text-databricks-gray-600">Created:</span>
                      <span className="ml-2 text-databricks-gray-900">
                        {formatDate(build.created_at)}
                      </span>
                    </div>
                    {build.updated_at && (
                      <div>
                        <span className="text-databricks-gray-600">Updated:</span>
                        <span className="ml-2 text-databricks-gray-900">
                          {formatDate(build.updated_at)}
                        </span>
                      </div>
                    )}
                  </div>

                  {build.config && (
                    <div className="mb-4 p-3 bg-gray-50 rounded text-sm">
                      <p className="text-databricks-gray-600 mb-1">Configuration:</p>
                      <div className="space-y-1">
                        {build.config.strategies && (
                          <p>
                            <span className="font-medium">Strategies:</span>{' '}
                            {Object.keys(build.config.strategies).join(', ')}
                          </p>
                        )}
                        {build.config.embedding_model_endpoint && (
                          <p>
                            <span className="font-medium">Embedding Model:</span>{' '}
                            {build.config.embedding_model_endpoint}
                          </p>
                        )}
                      </div>
                    </div>
                  )}

                  {build.job_url && (
                    <div className="mb-4">
                      <a
                        href={build.job_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center text-sm text-databricks-blue hover:underline"
                      >
                        <ExternalLink className="w-4 h-4 mr-1" />
                        View Job in Databricks
                      </a>
                    </div>
                  )}

                  <div className="flex items-center gap-3 pt-4 border-t">
                    {build.state === 'SUCCESS' ? (
                      <Button
                        variant="primary"
                        size="sm"
                        onClick={() => handleEvaluate(build.run_id)}
                        icon={<Play className="w-4 h-4" />}
                      >
                        Evaluate This Build
                      </Button>
                    ) : build.state === 'RUNNING' || build.state === 'PENDING' ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={loadProjectDetails}
                        icon={<RefreshCw className="w-4 h-4" />}
                      >
                        Refresh Status
                      </Button>
                    ) : build.state === 'FAILED' ? (
                      <div className="flex items-center gap-3">
                        <Button
                          variant="primary"
                          size="sm"
                          onClick={handleCreateBuild}
                          icon={<RefreshCw className="w-4 h-4" />}
                        >
                          Retry Build
                        </Button>
                        {build.job_url && (
                          <a
                            href={build.job_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-databricks-blue hover:underline"
                          >
                            View Error Logs
                          </a>
                        )}
                      </div>
                    ) : null}
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>

      {/* Info Box */}
      {builds.length > 0 && (
        <Card className="bg-databricks-gray-50">
          <CardContent className="p-4">
            <h3 className="text-sm font-semibold text-databricks-gray-900 mb-2">
              Next Steps
            </h3>
            <ul className="space-y-1 text-sm text-databricks-gray-700">
              <li>• Create a new build with different strategies to compare performance</li>
              <li>• Evaluate successful builds to measure retrieval quality</li>
              <li>• View detailed metrics and analytics in the MLflow experiment</li>
              <li>• Compare multiple builds in the Review section</li>
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
