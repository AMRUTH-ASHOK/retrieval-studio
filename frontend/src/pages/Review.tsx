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
} from '@mui/material'
import { evaluationsApi } from '../services/evaluations'

export default function Review() {
  const [runId, setRunId] = useState('')
  const [results, setResults] = useState<any[]>([])

  const loadResults = async () => {
    if (!runId) return

    try {
      const data = await evaluationsApi.getResults(runId)
      setResults(data)
    } catch (error) {
      console.error('Failed to load results:', error)
    }
  }

  useEffect(() => {
    if (runId) {
      loadResults()
    }
  }, [runId])

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Review Evaluation Results
      </Typography>

      <Paper sx={{ p: 3, mt: 3 }}>
        <TextField
          fullWidth
          label="Run ID"
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
          sx={{ mb: 3 }}
          placeholder="Enter run ID to view results"
        />

        {results.length > 0 && (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Query</TableCell>
                  <TableCell>Strategy</TableCell>
                  <TableCell>Metrics</TableCell>
                  <TableCell>Latency (ms)</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {results.map((result, index) => (
                  <TableRow key={index}>
                    <TableCell>{result.query_text}</TableCell>
                    <TableCell>{result.strategy}</TableCell>
                    <TableCell>
                      {result.metrics ? JSON.stringify(JSON.parse(result.metrics), null, 2) : '-'}
                    </TableCell>
                    <TableCell>{result.retrieval_latency_ms?.toFixed(2)}</TableCell>
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
