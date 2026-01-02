import api from './api'
import { Evaluation } from '../types'

export const evaluationsApi = {
  create: async (data: { run_id: string; queries_table: string }): Promise<Evaluation> => {
    const response = await api.post('/evaluations', data)
    return response.data
  },

  getResults: async (runId: string): Promise<any[]> => {
    const response = await api.get(`/evaluations/${runId}/results`)
    return response.data
  },
}
