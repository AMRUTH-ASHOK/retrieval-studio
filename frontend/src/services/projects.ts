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

  delete: async (projectId: string): Promise<{ success: boolean; message: string; project_id: string }> => {
    const response = await api.delete(`/projects/${projectId}`)
    return response.data
  },

  getMLflowExperiment: async (projectId: string): Promise<{
    experiment_name: string
    mlflow_url: string
    workspace_url: string
  }> => {
    const response = await api.get(`/projects/${projectId}/mlflow`)
    return response.data
  },

  getMLflowRuns: async (projectId: string): Promise<{
    experiment_name: string
    experiment_id: string
    runs: Array<{
      run_id: string
      run_name: string
      status: string
      start_time: number
      end_time: number | null
      role: string
      metrics: Record<string, number>
      params: Record<string, string>
      tags: Record<string, string>
    }>
  }> => {
    const response = await api.get(`/projects/${projectId}/mlflow/runs`)
    return response.data
  },
}
