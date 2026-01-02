import api from './api'
import { BuildJob, BuildJobConfig } from '../types'

export const buildsApi = {
  create: async (data: { project_id: string; config: BuildJobConfig }): Promise<BuildJob> => {
    const response = await api.post('/builds', data)
    return response.data
  },

  getById: async (runId: string): Promise<BuildJob> => {
    const response = await api.get(`/builds/${runId}`)
    return response.data
  },

  getByProject: async (projectId: string): Promise<BuildJob[]> => {
    const response = await api.get(`/builds/project/${projectId}`)
    return response.data
  },
}
