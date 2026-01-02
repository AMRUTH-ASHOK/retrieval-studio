import { useState } from 'react'
import {
  Box,
  Typography,
  Button,
  Paper,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from '@mui/material'
import { PlayArrow as PlayIcon } from '@mui/icons-material'
import { evaluationsApi } from '../services/evaluations'

export default function Evaluate() {
  const [runId, setRunId] = useState('')
  const [queriesTable, setQueriesTable] = useState('')

  const handleSubmit = async () => {
    try {
      await evaluationsApi.create({
        run_id: runId,
        queries_table: queriesTable,
      })
      alert('Evaluation job submitted successfully!')
    } catch (error) {
      console.error('Failed to submit evaluation:', error)
      alert('Failed to submit evaluation job')
    }
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Evaluate Retrieval Quality
      </Typography>

      <Paper sx={{ p: 3, mt: 3, maxWidth: 600 }}>
        <Typography variant="h6" gutterBottom>
          Start Evaluation
        </Typography>

        <TextField
          fullWidth
          label="Run ID"
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
          sx={{ mb: 2 }}
          helperText="Enter the build run ID to evaluate"
        />

        <TextField
          fullWidth
          label="Queries Table"
          value={queriesTable}
          onChange={(e) => setQueriesTable(e.target.value)}
          sx={{ mb: 3 }}
          placeholder="catalog.schema.table_name"
          helperText="Delta table containing evaluation queries"
        />

        <Button
          variant="contained"
          startIcon={<PlayIcon />}
          onClick={handleSubmit}
          disabled={!runId || !queriesTable}
        >
          Run Evaluation
        </Button>
      </Paper>
    </Box>
  )
}
