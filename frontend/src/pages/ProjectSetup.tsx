import { useState } from 'react'
import { Plus } from 'lucide-react'
import { projectsApi } from '../services/projects'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Modal } from '../components/ui/Modal'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '../components/ui/Table'
import { Card } from '../components/ui/Card'

export default function ProjectSetup() {
  const { projects, loadProjects, isLoading, setSelectedProjectId } = useProject()
  const [open, setOpen] = useState(false)
  const [projectName, setProjectName] = useState('')
  const [description, setDescription] = useState('')
  const [isCreating, setIsCreating] = useState(false)
  const [error, setError] = useState('')

  const handleCreateProject = async () => {
    if (!projectName.trim()) {
      setError('Project name is required')
      return
    }

    setIsCreating(true)
    setError('')
    
    try {
      const newProject = await projectsApi.create({
        project_name: projectName,
        description: description || undefined,
      })
      setOpen(false)
      setProjectName('')
      setDescription('')
      await loadProjects()
      setSelectedProjectId(newProject.project_id)
    } catch (error) {
      console.error('Failed to create project:', error)
      setError('Failed to create project. Please try again.')
    } finally {
      setIsCreating(false)
    }
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-databricks-gray-900">Projects</h1>
          <p className="text-sm text-databricks-gray-600 mt-1">
            Manage your retrieval pipeline projects
          </p>
        </div>
        <Button
          variant="primary"
          onClick={() => setOpen(true)}
          icon={<Plus className="w-4 h-4" />}
        >
          New Project
        </Button>
      </div>

      {isLoading ? (
        <Card>
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
          </div>
        </Card>
      ) : projects.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-databricks-gray-100 rounded-full flex items-center justify-center">
              <Plus className="w-8 h-8 text-databricks-gray-400" />
            </div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">
              No projects yet
            </h3>
            <p className="text-sm text-databricks-gray-600 mb-6">
              Create your first project to get started with retrieval pipelines
            </p>
            <Button
              variant="primary"
              onClick={() => setOpen(true)}
              icon={<Plus className="w-4 h-4" />}
            >
              Create Project
            </Button>
          </div>
        </Card>
      ) : (
        <Card padding={false}>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Project Name</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Created</TableHead>
                <TableHead>Last Updated</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {projects.map((project) => (
                <TableRow
                  key={project.project_id}
                  onClick={() => setSelectedProjectId(project.project_id)}
                >
                  <TableCell>
                    <span className="font-medium text-databricks-blue cursor-pointer hover:underline">
                      {project.project_name}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-600">
                      {project.description || '-'}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-600">
                      {new Date(project.created_at).toLocaleDateString()}
                    </span>
                  </TableCell>
                  <TableCell>
                    <span className="text-databricks-gray-600">
                      {new Date(project.updated_at).toLocaleDateString()}
                    </span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}

      <Modal
        isOpen={open}
        onClose={() => {
          setOpen(false)
          setError('')
          setProjectName('')
          setDescription('')
        }}
        title="Create New Project"
        footer={
          <>
            <Button
              variant="outline"
              onClick={() => {
                setOpen(false)
                setError('')
                setProjectName('')
                setDescription('')
              }}
              disabled={isCreating}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={handleCreateProject}
              isLoading={isCreating}
              disabled={!projectName.trim()}
            >
              Create Project
            </Button>
          </>
        }
      >
        <div className="space-y-4">
          <Input
            label="Project Name"
            value={projectName}
            onChange={(e) => setProjectName(e.target.value)}
            placeholder="Enter project name"
            required
            error={error && !projectName.trim() ? error : ''}
          />
          <div>
            <label className="block text-sm font-medium text-databricks-gray-700 mb-1">
              Description (optional)
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Enter project description"
              rows={3}
              className="w-full px-3 py-2 border border-databricks-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-blue focus:border-databricks-blue"
            />
          </div>
          {error && projectName.trim() && (
            <p className="text-sm text-databricks-error">{error}</p>
          )}
        </div>
      </Modal>
    </div>
  )
}
