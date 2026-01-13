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

  getStatus: async (runId: string): Promise<{
    run_id: string
    state: string
    job_url: string | null
    status: any
    start_time: number | null
  }> => {
    const response = await api.get(`/builds/${runId}/status`)
    return response.data
  },

  getResults: async (runId: string): Promise<{
    run_id: string
    results: {
      [strategy: string]: {
        chunks_table: string
        index_name: string
        chunk_count: number
        processing_time_seconds: number
      }
    } | null
    status: string
    message?: string
  }> => {
    const response = await api.get(`/builds/${runId}/results`)
    return response.data
  },
}
