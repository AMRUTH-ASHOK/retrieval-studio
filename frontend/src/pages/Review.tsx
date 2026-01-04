import { Box, Typography, Paper, Button } from '@mui/material'
import OpenInNewIcon from '@mui/icons-material/OpenInNew'

export default function Review() {
  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Review Evaluation Results
      </Typography>

      <Paper sx={{ p: 4, mt: 3, textAlign: 'center' }}>
        <Typography variant="h6" gutterBottom>
          Detailed Results are in MLflow
        </Typography>

        <Typography variant="body1" paragraph color="text.secondary" sx={{ mb: 4 }}>
          To review individual query results and detailed metrics per strategy, please access the MLflow Experiment run directly.
        </Typography>

        <Button
          variant="outlined"
          color="primary"
          endIcon={<OpenInNewIcon />}
          href="/#mlflow/experiments"
          target="_blank"
        >
          Open MLflow Experiments
        </Button>
      </Paper>
    </Box>
  )
}
