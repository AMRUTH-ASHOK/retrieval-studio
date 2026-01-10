import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import ProjectSetup from './pages/ProjectSetup'
import ProjectDetails from './pages/ProjectDetails'
import Build from './pages/Build'
import Evaluate from './pages/Evaluate'
import Review from './pages/Review'
import Leaderboard from './pages/Leaderboard'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Navigate to="/projects" replace />} />
        <Route path="/projects" element={<ProjectSetup />} />
        <Route path="/projects/:projectId" element={<ProjectDetails />} />
        <Route path="/build" element={<Build />} />
        <Route path="/evaluate" element={<Evaluate />} />
        <Route path="/review" element={<Review />} />
        <Route path="/leaderboard" element={<Leaderboard />} />
      </Routes>
    </Layout>
  )
}

export default App
