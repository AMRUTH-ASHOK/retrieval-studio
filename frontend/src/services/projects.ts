import api from './api'
import { Project } from '../types'

export const projectsApi = {
  getAll: async (): Promise<Project[]> => {
    const response = await api.get('/projects')
    return response.data
  },

  getById: async (projectId: string): Promise<Project> => {
    const response = await api.get(`/projects/${projectId}`)
    return response.data
  },

  create: async (data: { project_name: string; description?: string }): Promise<Project> => {
    const response = await api.post('/projects', data)
    return response.data
  },
}
