import { useState, useEffect } from 'react'
import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Grid,
} from '@mui/material'
import { leaderboardApi } from '../services/leaderboard'
import { LeaderboardEntry } from '../types'
import MetricCard from '../components/MetricCard'

export default function Leaderboard() {
  const [runId, setRunId] = useState('')
  const [entries, setEntries] = useState<LeaderboardEntry[]>([])

  const loadLeaderboard = async () => {
    if (!runId) return

    try {
      const data = await leaderboardApi.getByRun(runId)
      setEntries(data)
    } catch (error) {
      console.error('Failed to load leaderboard:', error)
    }
  }

  useEffect(() => {
    if (runId) {
      loadLeaderboard()
    }
  }, [runId])

  const topStrategy = entries.length > 0 ? entries[0] : null

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Leaderboard
      </Typography>

      <Paper sx={{ p: 3, mt: 3 }}>
        <TextField
          fullWidth
          label="Run ID"
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
          sx={{ mb: 3 }}
          placeholder="Enter run ID to view leaderboard"
        />

        {topStrategy && (
          <Grid container spacing={2} sx={{ mb: 3 }}>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="Top Strategy"
                value={topStrategy.strategy}
                subtitle={`Recall@10: ${(topStrategy.avg_recall_at_10 || 0).toFixed(3)}`}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="Avg Recall@10"
                value={(topStrategy.avg_recall_at_10 || 0).toFixed(3)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="Avg NDCG@10"
                value={(topStrategy.avg_ndcg_at_10 || 0).toFixed(3)}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <MetricCard
                title="Avg Latency"
                value={`${(topStrategy.avg_latency_ms || 0).toFixed(0)} ms`}
              />
            </Grid>
          </Grid>
        )}

        {entries.length > 0 && (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Rank</TableCell>
                  <TableCell>Strategy</TableCell>
                  <TableCell align="right">Recall@5</TableCell>
                  <TableCell align="right">Recall@10</TableCell>
                  <TableCell align="right">NDCG@5</TableCell>
                  <TableCell align="right">NDCG@10</TableCell>
                  <TableCell align="right">Avg Latency (ms)</TableCell>
                  <TableCell align="right">Queries</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {entries.map((entry, index) => (
                  <TableRow key={entry.strategy}>
                    <TableCell>{index + 1}</TableCell>
                    <TableCell>{entry.strategy}</TableCell>
                    <TableCell align="right">
                      {entry.avg_recall_at_5?.toFixed(3) || '-'}
                    </TableCell>
                    <TableCell align="right">
                      {entry.avg_recall_at_10?.toFixed(3) || '-'}
                    </TableCell>
                    <TableCell align="right">
                      {entry.avg_ndcg_at_5?.toFixed(3) || '-'}
                    </TableCell>
                    <TableCell align="right">
                      {entry.avg_ndcg_at_10?.toFixed(3) || '-'}
                    </TableCell>
                    <TableCell align="right">
                      {entry.avg_latency_ms?.toFixed(0) || '-'}
                    </TableCell>
                    <TableCell align="right">{entry.num_queries}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Paper>
    </Box>
  )
}
