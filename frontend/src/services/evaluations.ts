import api from './api'
import { Evaluation } from '../types'

export const evaluationsApi = {
  create: async (data: {
    run_id: string
    queries_table?: string
    corpus_table?: string
    dataset_type?: string
    top_k?: number
    auto_generate_queries?: boolean
    num_queries?: number
    query_style?: string
    compare_query_types?: boolean
    judge_model_endpoint?: string
  }): Promise<Evaluation> => {
    const response = await api.post('/evaluations', data)
    return response.data
  },

  getStatus: async (runId: string): Promise<{
    run_id: string
    state: string
    job_url: string | null
    status: any
    start_time: number | null
  }> => {
    const response = await api.get(`/evaluations/${runId}/status`)
    return response.data
  },

  getResults: async (runId: string): Promise<any[]> => {
    const response = await api.get(`/evaluations/${runId}/results`)
    return response.data
  },

  getByBuildRun: async (buildRunId: string): Promise<Evaluation[]> => {
    const response = await api.get(`/evaluations/build/${buildRunId}`)
    return response.data
  },
}
