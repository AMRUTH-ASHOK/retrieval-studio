import api from './api'
import { LeaderboardEntry } from '../types'

export const leaderboardApi = {
  getByRun: async (runId: string): Promise<LeaderboardEntry[]> => {
    const response = await api.get(`/leaderboard/${runId}`)
    return response.data
  },

  getByProject: async (projectId: string): Promise<any[]> => {
    const response = await api.get(`/leaderboard/project/${projectId}/aggregate`)
    return response.data
  },
}
