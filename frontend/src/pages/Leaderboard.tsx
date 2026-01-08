import { useState, useEffect } from 'react'
import { Trophy, TrendingUp, Clock, Target } from 'lucide-react'
import { evaluationsApi } from '../services/evaluations'
import { buildsApi } from '../services/builds'
import { useProject } from '../context/ProjectContext'
import { Select } from '../components/ui/Select'
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/Card'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '../components/ui/Table'
import { Badge } from '../components/ui/Badge'
import { LeaderboardEntry, BuildJob } from '../types'

export default function Leaderboard() {
  const { selectedProject, selectedProjectId } = useProject()
  const [builds, setBuilds] = useState<BuildJob[]>([])
  const [selectedRun, setSelectedRun] = useState('')
  const [leaderboardData, setLeaderboardData] = useState<LeaderboardEntry[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (selectedProjectId) {
      loadBuilds()
    }
  }, [selectedProjectId])

  useEffect(() => {
    if (selectedRun) {
      loadLeaderboard()
    }
  }, [selectedRun])

  const loadBuilds = async () => {
    try {
      const buildsData = await buildsApi.list()
      const projectBuilds = buildsData.filter(
        (b: BuildJob) => b.project_id === selectedProjectId && b.state === 'SUCCESS'
      )
      setBuilds(projectBuilds)
      if (projectBuilds.length > 0 && !selectedRun) {
        setSelectedRun(projectBuilds[0].run_id)
      }
    } catch (error) {
      console.error('Failed to load builds:', error)
      setError('Failed to load builds')
    }
  }

  const loadLeaderboard = async () => {
    setIsLoading(true)
    setError('')
    try {
      // Fetch evaluation results for this build run
      const data = await evaluationsApi.getResults(selectedRun)
      
      // Aggregate by strategy
      const strategyMap: Record<string, any> = {}
      
      data.forEach((result: any) => {
        const strategy = result.strategy
        if (!strategyMap[strategy]) {
          strategyMap[strategy] = {
            strategy,
            recalls: [],
            ndcgs: [],
            latencies: [],
            num_queries: 0
          }
        }
        
        // Parse metrics
        const metrics = typeof result.metrics === 'string' 
          ? JSON.parse(result.metrics) 
          : result.metrics
        
        strategyMap[strategy].recalls.push(metrics.recall_at_10 || 0)
        strategyMap[strategy].ndcgs.push(metrics.ndcg_at_10 || 0)
        strategyMap[strategy].latencies.push(metrics.retrieval_latency_ms || 0)
        strategyMap[strategy].num_queries++
      })
      
      // Calculate averages
      const leaderboard = Object.values(strategyMap).map((s: any) => ({
        strategy: s.strategy,
        avg_recall_at_10: s.recalls.reduce((a: number, b: number) => a + b, 0) / s.recalls.length,
        avg_ndcg_at_10: s.ndcgs.reduce((a: number, b: number) => a + b, 0) / s.ndcgs.length,
        avg_latency_ms: s.latencies.reduce((a: number, b: number) => a + b, 0) / s.latencies.length,
        num_queries: s.num_queries
      }))
      
      // Sort by avg_recall_at_10 descending
      leaderboard.sort((a, b) => (b.avg_recall_at_10 || 0) - (a.avg_recall_at_10 || 0))
      
      setLeaderboardData(leaderboard)
    } catch (error) {
      console.error('Failed to load leaderboard:', error)
      setError('Failed to load leaderboard data. Make sure evaluation has been run.')
      setLeaderboardData([])
    } finally {
      setIsLoading(false)
    }
  }

  const getBestStrategy = (metric: keyof LeaderboardEntry) => {
    if (leaderboardData.length === 0) return null
    return leaderboardData.reduce((best, current) => {
      const currentValue = current[metric] as number
      const bestValue = best[metric] as number
      return currentValue > bestValue ? current : best
    })
  }

  const formatMetric = (value: number | null | undefined, decimals = 3) => {
    if (value === null || value === undefined) return '-'
    return value.toFixed(decimals)
  }

  const getTopPerformer = () => {
    if (leaderboardData.length === 0) return null
    // Calculate average of all metrics for ranking
    return leaderboardData.reduce((best, current) => {
      const currentAvg =
        ((current.avg_recall_at_10 || 0) +
          (current.avg_ndcg_at_10 || 0)) /
        2
      const bestAvg =
        ((best.avg_recall_at_10 || 0) +
          (best.avg_ndcg_at_10 || 0)) /
        2
      return currentAvg > bestAvg ? current : best
    })
  }

  const topPerformer = getTopPerformer()

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Leaderboard</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">
          Compare retrieval strategy performance across metrics
        </p>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to view leaderboard.
          </p>
        </div>
      )}

      {selectedProject && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
          <p className="text-sm text-blue-900">
            <span className="font-medium">Current project:</span> {selectedProject.project_name}
          </p>
        </div>
      )}

      {/* Run Selector */}
      {builds.length > 0 && (
        <Card className="mb-6">
          <Select
            label="Select Build Run"
            value={selectedRun}
            onChange={(e) => setSelectedRun(e.target.value)}
            options={builds.map((build) => ({
              value: build.run_id,
              label: `${build.run_id.substring(0, 12)}... - ${new Date(
                build.created_at
              ).toLocaleDateString()}`,
            }))}
          />
        </Card>
      )}

      {/* Metrics Summary */}
      {leaderboardData.length > 0 && topPerformer && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium text-databricks-gray-600">
                  Top Strategy
                </CardTitle>
                <Trophy className="w-5 h-5 text-databricks-warning" />
              </div>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold text-databricks-gray-900">
                {topPerformer.strategy}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium text-databricks-gray-600">
                  Best Recall@10
                </CardTitle>
                <TrendingUp className="w-5 h-5 text-databricks-success" />
              </div>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold text-databricks-gray-900">
                {formatMetric(getBestStrategy('avg_recall_at_10')?.avg_recall_at_10)}
              </p>
              <p className="text-xs text-databricks-gray-500 mt-1">
                {getBestStrategy('avg_recall_at_10')?.strategy}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium text-databricks-gray-600">
                  Best NDCG@10
                </CardTitle>
                <Target className="w-5 h-5 text-databricks-blue" />
              </div>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold text-databricks-gray-900">
                {formatMetric(getBestStrategy('avg_ndcg_at_10')?.avg_ndcg_at_10)}
              </p>
              <p className="text-xs text-databricks-gray-500 mt-1">
                {getBestStrategy('avg_ndcg_at_10')?.strategy}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium text-databricks-gray-600">
                  Fastest
                </CardTitle>
                <Clock className="w-5 h-5 text-databricks-primary" />
              </div>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold text-databricks-gray-900">
                {formatMetric(
                  Math.min(...leaderboardData.map((d) => d.avg_latency_ms || Infinity)),
                  0
                )}
                ms
              </p>
              <p className="text-xs text-databricks-gray-500 mt-1">
                {
                  leaderboardData.reduce((fastest, current) =>
                    (current.avg_latency_ms || Infinity) < (fastest.avg_latency_ms || Infinity)
                      ? current
                      : fastest
                  ).strategy
                }
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Leaderboard Table */}
      {isLoading ? (
        <Card>
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-databricks-blue"></div>
          </div>
        </Card>
      ) : error ? (
        <Card>
          <div className="text-center py-12">
            <p className="text-sm text-databricks-error">{error}</p>
          </div>
        </Card>
      ) : leaderboardData.length === 0 ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-databricks-gray-100 rounded-full flex items-center justify-center">
              <Trophy className="w-8 h-8 text-databricks-gray-400" />
            </div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">
              No evaluation results yet
            </h3>
            <p className="text-sm text-databricks-gray-600">
              Run an evaluation to see strategy comparisons
            </p>
          </div>
        </Card>
      ) : (
        <Card padding={false}>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Rank</TableHead>
                <TableHead>Strategy</TableHead>
                <TableHead>Recall@10</TableHead>
                <TableHead>NDCG@10</TableHead>
                <TableHead>Latency (ms)</TableHead>
                <TableHead>Queries</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {leaderboardData.map((entry, index) => (
                <TableRow key={entry.strategy}>
                  <TableCell>
                    {index === 0 && (
                      <Trophy className="w-5 h-5 text-databricks-warning inline mr-2" />
                    )}
                    <span className="font-medium">#{index + 1}</span>
                  </TableCell>
                  <TableCell>
                    <span className="font-medium text-databricks-gray-900">
                      {entry.strategy}
                    </span>
                  </TableCell>
                  <TableCell>{formatMetric(entry.avg_recall_at_10)}</TableCell>
                  <TableCell>{formatMetric(entry.avg_ndcg_at_10)}</TableCell>
                  <TableCell>{formatMetric(entry.avg_latency_ms, 0)}</TableCell>
                  <TableCell>
                    <Badge variant="default">{entry.num_queries}</Badge>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}

      {/* Info Box */}
      {leaderboardData.length > 0 && (
        <Card className="mt-6 bg-databricks-gray-50">
          <h3 className="text-sm font-semibold text-databricks-gray-900 mb-3">
            Understanding Metrics
          </h3>
          <ul className="space-y-2 text-sm text-databricks-gray-700">
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Recall@K:</strong> Proportion of relevant items retrieved in top K results
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>NDCG@K:</strong> Normalized Discounted Cumulative Gain - measures ranking
                quality
              </span>
            </li>
            <li className="flex items-start">
              <span className="mr-2">•</span>
              <span>
                <strong>Latency:</strong> Average time taken for retrieval in milliseconds
              </span>
            </li>
          </ul>
        </Card>
      )}
    </div>
  )
}
