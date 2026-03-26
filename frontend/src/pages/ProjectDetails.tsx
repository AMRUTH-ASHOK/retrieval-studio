import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Plus, ExternalLink, Play, RefreshCw, CheckCircle, XCircle, Clock, AlertCircle, ChevronDown, ChevronRight, Trash2, Database, Shield, BookOpen } from 'lucide-react'
import { projectsApi } from '../services/projects'
import { buildsApi } from '../services/builds'
import { evaluationsApi } from '../services/evaluations'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Modal } from '../components/ui/Modal'
import api from '../services/api'
import type { Project, BuildJob, Evaluation } from '../types'

export default function ProjectDetails() {
  const { projectId } = useParams<{ projectId: string }>()
  const navigate = useNavigate()
  const { loadProjects, setSelectedProjectId } = useProject()
  const [project, setProject] = useState<Project | null>(null)
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [evaluations, setEvaluations] = useState<Record<string, Evaluation[]>>({})
  const [expandedBuilds, setExpandedBuilds] = useState<Record<string, boolean>>({})
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')
  const [showDeleteModal, setShowDeleteModal] = useState(false)
  const [isDeleting, setIsDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState('')

  // Resource management state
  const [indexSelections, setIndexSelections] = useState<any[]>([])
  const [cleanupPreview, setCleanupPreview] = useState<any>(null)
  const [isCleaningUp, setIsCleaningUp] = useState(false)
  const [cleanupResult, setCleanupResult] = useState<any>(null)
  const [showCleanupModal, setShowCleanupModal] = useState(false)

  // Study state
  const [studies, setStudies] = useState<any[]>([])
  const [showNewStudy, setShowNewStudy] = useState(false)
  const [newStudyName, setNewStudyName] = useState('')
  const [newStudyDesc, setNewStudyDesc] = useState('')

  useEffect(() => {
    if (projectId) {
      loadProjectDetails()
      loadIndexSelections()
      loadStudies()
    }
  }, [projectId])

  const loadIndexSelections = async () => {
    if (!projectId) return
    try {
      const response = await api.get(`/cleanup/projects/${projectId}/indexes`)
      setIndexSelections(response.data || [])
    } catch { setIndexSelections([]) }
  }

  const loadStudies = async () => {
    if (!projectId) return
    try {
      const response = await api.get(`/studies/project/${projectId}`)
      setStudies(response.data || [])
    } catch { setStudies([]) }
  }

  const handleToggleIndexStatus = async (selectionId: string, currentStatus: string) => {
    const newStatus = currentStatus === 'keep' ? 'discard' : currentStatus === 'discard' ? 'keep' : 'keep'
    try {
      await api.put(`/cleanup/projects/${projectId}/indexes/status`, { updates: [{ id: selectionId, status: newStatus }] })
      loadIndexSelections()
    } catch (e) { console.error('Failed to update:', e) }
  }

  const handlePreviewCleanup = async () => {
    if (!projectId) return
    try {
      const response = await api.get(`/cleanup/projects/${projectId}/cleanup/preview`)
      setCleanupPreview(response.data)
      if (response.data.count > 0) setShowCleanupModal(true)
      else setError('No resources marked for cleanup. Mark indexes as "discard" first.')
    } catch (e) { console.error('Preview failed:', e) }
  }

  const handleRunCleanup = async () => {
    if (!projectId) return
    setIsCleaningUp(true)
    try {
      const response = await api.post(`/cleanup/projects/${projectId}/cleanup`)
      setCleanupResult(response.data)
      setShowCleanupModal(false)

      if (response.data.job_run_id) {
        const pollCleanup = setInterval(async () => {
          try {
            const statusResp = await api.get(`/builds/${response.data.job_run_id}/status`)
            const state = statusResp.data?.state
            if (state === 'SUCCESS' || state === 'FAILED') {
              clearInterval(pollCleanup)
              setIsCleaningUp(false)
              setCleanupResult((prev: any) => ({ ...prev, state, completed: true }))
              loadIndexSelections()
            }
          } catch { /* keep polling */ }
        }, 5000)
      } else {
        setIsCleaningUp(false)
        loadIndexSelections()
      }
    } catch (e) {
      console.error('Cleanup failed:', e)
      setIsCleaningUp(false)
    }
  }

  const handleCreateStudy = async () => {
    if (!projectId || !newStudyName) return
    try {
      await api.post('/studies/', { project_id: projectId, study_name: newStudyName, description: newStudyDesc })
      setNewStudyName('')
      setNewStudyDesc('')
      setShowNewStudy(false)
      loadStudies()
    } catch (e) { console.error('Failed to create study:', e) }
  }

  const handleDeleteStudy = async (studyId: string) => {
    try {
      await api.delete(`/studies/${studyId}`)
      loadStudies()
    } catch (e) { console.error('Failed to delete study:', e) }
  }

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

      // Load evaluations for each build
      const evalsMap: Record<string, Evaluation[]> = {}
      await Promise.all(
        sortedBuilds.map(async (build) => {
          try {
            const buildEvals = await evaluationsApi.getByBuildRun(build.run_id)
            evalsMap[build.run_id] = buildEvals
          } catch (error) {
            console.error(`Failed to load evaluations for build ${build.run_id}:`, error)
            evalsMap[build.run_id] = []
          }
        })
      )
      setEvaluations(evalsMap)
    } catch (err: any) {
      console.error('Failed to load project details:', err)
      setError(err?.response?.data?.detail || 'Failed to load project details')
    } finally {
      setIsLoading(false)
    }
  }

  const toggleBuildExpand = (runId: string) => {
    setExpandedBuilds(prev => ({
      ...prev,
      [runId]: !prev[runId]
    }))
  }

  const handleCreateBuild = () => {
    navigate('/build')
  }

  const handleEvaluate = (runId: string) => {
    navigate('/evaluate')
  }

  const handleDeleteProject = async () => {
    if (!projectId) return

    setIsDeleting(true)
    setDeleteError('')

    try {
      await projectsApi.delete(projectId)
      // Clear selected project
      setSelectedProjectId(null)
      // Refresh projects list
      await loadProjects()
      // Navigate back to projects page
      navigate('/projects')
    } catch (err: any) {
      console.error('Failed to delete project:', err)
      setDeleteError(err?.response?.data?.detail || 'Failed to delete project. Please try again.')
    } finally {
      setIsDeleting(false)
    }
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
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-2xl font-semibold text-databricks-gray-900">
                {project.project_name}
              </h1>
            </div>
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

          <div className="flex flex-col gap-2">
            <Button
              variant="primary"
              onClick={handleCreateBuild}
              icon={<Plus className="w-4 h-4" />}
              className="w-full"
            >
              Create New Build
            </Button>
            <Button
              variant="outline"
              onClick={() => setShowDeleteModal(true)}
              icon={<Trash2 className="w-4 h-4" />}
              className="w-full border-2 border-red-500 text-red-600 hover:text-white hover:bg-red-600 font-medium"
            >
              Delete Project
            </Button>
          </div>
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

                  <div className="pt-4 border-t space-y-3">
                    <div className="flex items-center gap-3">
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

                    {/* Evaluation History - Always show for successful builds */}
                    {build.state === 'SUCCESS' && (
                      <div className="border-t pt-3">
                        <button
                          onClick={() => toggleBuildExpand(build.run_id)}
                          className="flex items-center gap-2 text-sm font-medium text-databricks-gray-900 hover:text-databricks-blue"
                        >
                          {expandedBuilds[build.run_id] ? (
                            <ChevronDown className="w-4 h-4" />
                          ) : (
                            <ChevronRight className="w-4 h-4" />
                          )}
                          <span>
                            Evaluation History ({evaluations[build.run_id]?.length || 0})
                          </span>
                        </button>

                        {expandedBuilds[build.run_id] && (
                          <div className="mt-3 space-y-2">
                            {evaluations[build.run_id] && evaluations[build.run_id].length > 0 ? (
                              evaluations[build.run_id].map((evaluation) => (
                                <div
                                  key={evaluation.eval_id}
                                  className="p-3 bg-gray-50 border border-gray-200 rounded-lg"
                                >
                                  <div className="flex items-center justify-between mb-2">
                                    <div className="flex items-center gap-2">
                                      <span className="text-sm font-mono text-databricks-gray-700">
                                        {evaluation.eval_id?.substring(0, 8)}...
                                      </span>
                                      {getStatusBadge(evaluation.state)}
                                    </div>
                                    {evaluation.job_url && (
                                      <a
                                        href={evaluation.job_url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="inline-flex items-center text-xs text-databricks-blue hover:underline"
                                      >
                                        <ExternalLink className="w-3 h-3 mr-1" />
                                        View Job
                                      </a>
                                    )}
                                  </div>
                                  <div className="text-xs text-databricks-gray-600">
                                    Created: {formatDate(evaluation.created_at)}
                                  </div>
                                </div>
                              ))
                            ) : (
                              <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg text-center">
                                <p className="text-sm text-blue-900 mb-2">
                                  No evaluations yet for this build
                                </p>
                                <Button
                                  variant="primary"
                                  size="sm"
                                  onClick={() => handleEvaluate(build.run_id)}
                                  icon={<Play className="w-4 h-4" />}
                                >
                                  Run First Evaluation
                                </Button>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
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

      {/* Resource Management Section */}
      {indexSelections.length > 0 && (
        <Card className="mt-6">
          <CardContent className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <Database className="w-5 h-5 text-databricks-blue" />
                <h2 className="text-lg font-semibold text-databricks-gray-900">Resource Management</h2>
              </div>
              <Button variant="outline" size="sm" onClick={handlePreviewCleanup}>
                <Trash2 className="w-4 h-4 mr-1" /> Cleanup Discarded
              </Button>
            </div>

            {cleanupResult && (
              <div className={`mb-4 p-3 rounded-md border ${cleanupResult.completed && cleanupResult.state === 'SUCCESS' ? 'bg-green-50 border-green-200' : cleanupResult.completed && cleanupResult.state === 'FAILED' ? 'bg-red-50 border-red-200' : 'bg-blue-50 border-blue-200'}`}>
                <p className={`text-sm ${cleanupResult.completed && cleanupResult.state === 'SUCCESS' ? 'text-green-800' : cleanupResult.completed && cleanupResult.state === 'FAILED' ? 'text-red-800' : 'text-blue-800'}`}>
                  {cleanupResult.completed && cleanupResult.state === 'SUCCESS'
                    ? `Cleanup complete! ${cleanupResult.count} resources deleted.`
                    : cleanupResult.completed && cleanupResult.state === 'FAILED'
                    ? 'Cleanup job failed. Check the Databricks job for details.'
                    : isCleaningUp
                    ? `Cleanup running... ${cleanupResult.count} resources queued for deletion.`
                    : `Cleanup job submitted. ${cleanupResult.count} resources queued.`}
                  {cleanupResult.job_url && (
                    <a href={cleanupResult.job_url} target="_blank" rel="noopener noreferrer" className="ml-2 underline">View job</a>
                  )}
                </p>
              </div>
            )}

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-databricks-gray-200">
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Source</th>
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Strategy</th>
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Index Name</th>
                    <th className="text-center py-2 px-3 text-databricks-gray-700">Status</th>
                    <th className="text-center py-2 px-3 text-databricks-gray-700">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {indexSelections.map((idx: any) => (
                    <tr key={idx.id} className="border-b border-databricks-gray-100">
                      <td className="py-2 px-3 font-medium">{idx.source_name || '-'}</td>
                      <td className="py-2 px-3">{idx.strategy_name || '-'}</td>
                      <td className="py-2 px-3 font-mono text-xs text-databricks-gray-600 max-w-[200px] truncate">{idx.index_name}</td>
                      <td className="py-2 px-3 text-center">
                        <Badge variant={idx.status === 'keep' ? 'success' : idx.status === 'discard' ? 'error' : idx.status === 'deleted' ? 'warning' : 'secondary'}>
                          {idx.status}
                        </Badge>
                      </td>
                      <td className="py-2 px-3 text-center">
                        {idx.status !== 'deleted' && (
                          <button onClick={() => handleToggleIndexStatus(idx.id, idx.status)}
                            className={`px-2 py-1 text-xs font-medium rounded ${idx.status === 'keep' ? 'bg-red-100 text-red-700 hover:bg-red-200' : 'bg-green-100 text-green-700 hover:bg-green-200'}`}>
                            {idx.status === 'keep' ? 'Mark Discard' : 'Mark Keep'}
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Studies Section */}
      <Card className="mt-6">
        <CardContent className="p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <BookOpen className="w-5 h-5 text-databricks-blue" />
              <h2 className="text-lg font-semibold text-databricks-gray-900">Studies</h2>
            </div>
            <Button variant="outline" size="sm" onClick={() => setShowNewStudy(!showNewStudy)}>
              <Plus className="w-4 h-4 mr-1" /> New Study
            </Button>
          </div>

          {showNewStudy && (
            <div className="mb-4 p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200 space-y-3">
              <Input label="Study Name" value={newStudyName} onChange={(e) => setNewStudyName(e.target.value)} placeholder="e.g., initial_strategies" required />
              <Input label="Description" value={newStudyDesc} onChange={(e) => setNewStudyDesc(e.target.value)} placeholder="Optional description" />
              <div className="flex gap-2">
                <Button variant="primary" size="sm" onClick={handleCreateStudy} disabled={!newStudyName}>Create</Button>
                <Button variant="outline" size="sm" onClick={() => setShowNewStudy(false)}>Cancel</Button>
              </div>
            </div>
          )}

          {studies.length === 0 && !showNewStudy ? (
            <p className="text-sm text-databricks-gray-500 text-center py-4">No studies yet. Create a study to group related builds and evaluations.</p>
          ) : (
            <div className="space-y-3">
              {studies.map((study: any) => (
                <div key={study.study_id} className="p-4 border border-databricks-gray-200 rounded-lg">
                  <div className="flex items-center justify-between">
                    <div>
                      <h4 className="font-medium text-databricks-gray-900">{study.study_name}</h4>
                      {study.description && <p className="text-xs text-databricks-gray-600 mt-1">{study.description}</p>}
                      <p className="text-xs text-databricks-gray-400 mt-1">Created: {new Date(study.created_at).toLocaleDateString()}</p>
                    </div>
                    <Button variant="ghost" size="sm" onClick={() => handleDeleteStudy(study.study_id)} className="text-databricks-error">
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Cleanup Confirmation Modal */}
      {showCleanupModal && cleanupPreview && (
        <Modal
          isOpen={showCleanupModal}
          onClose={() => setShowCleanupModal(false)}
          title="Confirm Resource Cleanup"
          onConfirm={handleRunCleanup}
          confirmText={isCleaningUp ? 'Cleaning up...' : `Delete ${cleanupPreview.count} resources`}
          confirmVariant="danger"
          isLoading={isCleaningUp}
        >
          <div className="space-y-4">
            <div className="p-4 bg-red-50 border border-red-200 rounded-md">
              <p className="text-sm text-red-900 font-medium">This action cannot be undone!</p>
              <p className="text-xs text-red-800 mt-1">The following Vector Search indexes and Delta tables will be permanently deleted:</p>
            </div>
            <div className="max-h-48 overflow-y-auto space-y-2">
              {cleanupPreview.indexes_to_delete?.map((idx: any, i: number) => (
                <div key={i} className="p-2 bg-databricks-gray-50 rounded text-xs">
                  <p className="font-medium">{idx.source_name} / {idx.strategy_name}</p>
                  <p className="text-databricks-gray-500 font-mono truncate">{idx.index_name}</p>
                </div>
              ))}
            </div>
          </div>
        </Modal>
      )}

      {/* Delete Confirmation Modal */}
      <Modal
        isOpen={showDeleteModal}
        onClose={() => {
          setShowDeleteModal(false)
          setDeleteError('')
        }}
        title="Delete Project"
        footer={
          <>
            <Button
              variant="outline"
              onClick={() => {
                setShowDeleteModal(false)
                setDeleteError('')
              }}
              disabled={isDeleting}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={handleDeleteProject}
              isLoading={isDeleting}
              className="bg-red-600 hover:bg-red-700 text-white"
            >
              {isDeleting ? 'Deleting...' : 'Delete Project'}
            </Button>
          </>
        }
      >
        <div className="space-y-4">
          <div className="p-4 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-900 font-medium mb-2">
              ⚠️ Warning: This action cannot be undone
            </p>
            <p className="text-sm text-red-800">
              Deleting this project will permanently remove:
            </p>
            <ul className="mt-2 ml-4 space-y-1 text-sm text-red-800">
              <li>• All builds ({builds.length} total)</li>
              <li>• All evaluations associated with this project</li>
              <li>• All project configuration and metadata</li>
            </ul>
          </div>

          <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-md">
            <p className="text-sm text-yellow-900">
              <strong>Note:</strong> This will NOT delete:
            </p>
            <ul className="mt-2 ml-4 space-y-1 text-sm text-yellow-800">
              <li>• Delta tables (chunks, indexes, eval results)</li>
              <li>• Vector Search indexes</li>
              <li>• MLflow experiment runs</li>
            </ul>
            <p className="text-xs text-yellow-700 mt-2">
              Use the Resource Management section above to clean up VS indexes and Delta tables before deleting the project.
            </p>
          </div>

          <div>
            <p className="text-sm text-databricks-gray-900">
              Are you sure you want to delete project <strong>"{project?.project_name}"</strong>?
            </p>
          </div>

          {deleteError && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-md">
              <p className="text-sm text-red-800">{deleteError}</p>
            </div>
          )}
        </div>
      </Modal>
    </div>
  )
}
