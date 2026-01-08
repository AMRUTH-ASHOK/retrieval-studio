import api from './api'

export const evaluationsApi = {
  create: async (data: { run_id: string; queries_table: string; top_k?: number }) => {
    const response = await api.post('/evaluations', data)
    return response.data
  },

  getResults: async (runId: string) => {
    const response = await api.get(`/evaluations/${runId}/results`)
    return response.data
  },
}
