import api from './api'
import { DataType, Strategy } from '../types'

export const metadataApi = {
  getDataTypes: async (): Promise<DataType[]> => {
    const response = await api.get('/metadata/data-types')
    return response.data
  },

  getStrategies: async (): Promise<Strategy[]> => {
    const response = await api.get('/metadata/strategies')
    return response.data
  },
}
