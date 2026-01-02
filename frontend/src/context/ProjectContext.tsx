import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { projectsApi } from '../services/projects'
import { Project } from '../types'

interface ProjectContextType {
  projects: Project[]
  selectedProject: Project | null
  selectedProjectId: string
  setSelectedProjectId: (id: string) => void
  loadProjects: () => Promise<void>
  isLoading: boolean
}

const ProjectContext = createContext<ProjectContextType | undefined>(undefined)

export function ProjectProvider({ children }: { children: ReactNode }) {
  const [projects, setProjects] = useState<Project[]>([])
  const [selectedProjectId, setSelectedProjectId] = useState<string>('')
  const [isLoading, setIsLoading] = useState(true)

  const loadProjects = async () => {
    setIsLoading(true)
    try {
      const data = await projectsApi.getAll()
      setProjects(data)
      if (data.length > 0 && !selectedProjectId) {
        setSelectedProjectId(data[0].project_id)
      }
    } catch (error) {
      console.error('Failed to load projects:', error)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    loadProjects()
  }, [])

  const selectedProject = projects.find(p => p.project_id === selectedProjectId) || null

  return (
    <ProjectContext.Provider
      value={{
        projects,
        selectedProject,
        selectedProjectId,
        setSelectedProjectId,
        loadProjects,
        isLoading,
      }}
    >
      {children}
    </ProjectContext.Provider>
  )
}

export function useProject() {
  const context = useContext(ProjectContext)
  if (context === undefined) {
    throw new Error('useProject must be used within a ProjectProvider')
  }
  return context
}
