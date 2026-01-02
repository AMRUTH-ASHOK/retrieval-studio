import { Routes, Route, Navigate } from 'react-router-dom'
import { Box } from '@mui/material'
import Layout from './components/Layout'
import ProjectSetup from './pages/ProjectSetup'
import Build from './pages/Build'
import Evaluate from './pages/Evaluate'
import Review from './pages/Review'
import Leaderboard from './pages/Leaderboard'

function App() {
  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <Layout>
        <Routes>
          <Route path="/" element={<Navigate to="/projects" replace />} />
          <Route path="/projects" element={<ProjectSetup />} />
          <Route path="/build" element={<Build />} />
          <Route path="/evaluate" element={<Evaluate />} />
          <Route path="/review" element={<Review />} />
          <Route path="/leaderboard" element={<Leaderboard />} />
        </Routes>
      </Layout>
    </Box>
  )
}

export default App
