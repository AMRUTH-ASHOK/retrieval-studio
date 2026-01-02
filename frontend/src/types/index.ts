export interface Project {
  project_id: string
  project_name: string
  description?: string
  created_at: string
  updated_at: string
}

export interface BuildJobConfig {
  data_type: string
  data_config: Record<string, any>
  strategies: Record<string, Record<string, any>>
  embedding_model_endpoint: string
  vs_endpoint_name: string
  create_index: boolean
}

export interface BuildJob {
  run_id: string
  project_id: string
  state: string
  job_id?: string
  config: Record<string, any>
  created_at: string
  updated_at: string
}

export interface Evaluation {
  eval_id: string
  run_id: string
  state: string
  job_id?: string
  created_at: string
  updated_at: string
}

export interface LeaderboardEntry {
  strategy: string
  avg_recall_at_5?: number
  avg_recall_at_10?: number
  avg_ndcg_at_5?: number
  avg_ndcg_at_10?: number
  avg_latency_ms?: number
  num_queries: number
}

export interface DataType {
  name: string
  display_name: string
  input_schema: Record<string, any>
  compatible_strategies: string[]
}

export interface Strategy {
  name: string
  display_name: string
  description: string
  parameters: Record<string, any>
}
