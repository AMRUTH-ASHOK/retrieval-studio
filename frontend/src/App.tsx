import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import ProjectSetup from './pages/ProjectSetup'
import ProjectDetails from './pages/ProjectDetails'
import Build from './pages/Build'
import Evaluate from './pages/Evaluate'
import Review from './pages/Review'

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
      </Routes>
    </Layout>
  )
}

export default App
