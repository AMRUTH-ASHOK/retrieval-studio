import { Box, Typography, Paper, Link, Button } from '@mui/material'
import OpenInNewIcon from '@mui/icons-material/OpenInNew'

export default function Leaderboard() {
  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Leaderboard
      </Typography>

      <Paper sx={{ p: 4, mt: 3, textAlign: 'center' }}>
        <Typography variant="h6" gutterBottom>
          View Results in MLflow Experiments
        </Typography>
        
        <Typography variant="body1" paragraph color="text.secondary" sx={{ mb: 4 }}>
          We have moved the leaderboard to the native Databricks MLflow UI. 
          This allows you to compare runs, visualize metrics (Recall vs NDCG), and drill down into strategy parameters side-by-side.
        </Typography>

        <Button 
          variant="contained" 
          color="primary" 
          endIcon={<OpenInNewIcon />}
          href="/#mlflow/experiments" 
          target="_blank"
          size="large"
        >
          Go to MLflow Experiments
        </Button>

        <Box sx={{ mt: 4, p: 2, bgcolor: 'background.default', borderRadius: 1, textAlign: 'left' }}>
          <Typography variant="subtitle2" gutterBottom>
            How to compare strategies:
          </Typography>
          <ol>
            <li>Navigate to the <strong>Experiments</strong> tab in Databricks.</li>
            <li>Select your Project Experiment.</li>
            <li>In the "Runs" table, select the child runs you want to compare.</li>
            <li>Click the blue <strong>"Compare"</strong> button.</li>
            <li>Use the "Parallel Coordinates Plot" or "Scatter Plot" to analyze performance.</li>
          </ol>
        </Box>
      </Paper>
    </Box>
  )
}
