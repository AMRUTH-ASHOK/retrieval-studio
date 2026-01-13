import { ReactNode } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import {
  FolderOpen,
  Hammer,
  FlaskConical,
  FileText,
  ChevronRight
} from 'lucide-react'
import { useProject } from '../context/ProjectContext'
import { Select } from './ui/Select'

const drawerWidth = 260

interface LayoutProps {
  children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { projects, selectedProjectId, setSelectedProjectId, isLoading } = useProject()

  const menuItems = [
    { text: 'Projects', icon: FolderOpen, path: '/projects' },
    { text: 'Build', icon: Hammer, path: '/build' },
    { text: 'Evaluate', icon: FlaskConical, path: '/evaluate' },
    { text: 'Review', icon: FileText, path: '/review' },
  ]

  const projectOptions = projects.map(p => ({
    value: p.project_id,
    label: p.project_name
  }))

  return (
    <div className="flex h-screen bg-databricks-gray-50">
      {/* Sidebar */}
      <div 
        className="bg-white border-r border-databricks-gray-200 flex flex-col"
        style={{ width: drawerWidth }}
      >
        {/* Logo/Header */}
        <div className="h-16 flex items-center px-6 border-b border-databricks-gray-200 bg-gradient-to-r from-databricks-blue to-databricks-blue-light">
          <div className="flex items-center gap-3">
            <svg className="w-8 h-8" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
              <defs>
                <linearGradient id="logoGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" style={{stopColor: '#FF3621', stopOpacity: 1}} />
                  <stop offset="100%" style={{stopColor: '#ffffff', stopOpacity: 1}} />
                </linearGradient>
              </defs>
              <path d="M50 5 L90 30 L90 70 L50 95 L10 70 L10 30 Z" fill="url(#logoGrad)"/>
              <ellipse cx="50" cy="35" rx="20" ry="7" fill="white" opacity="0.9"/>
              <path d="M30 35 L30 42 Q30 47 50 47 Q70 47 70 42 L70 35" fill="white" opacity="0.7"/>
              <circle cx="50" cy="65" r="8" fill="none" stroke="white" strokeWidth="2.5"/>
              <line x1="56" y1="71" x2="62" y2="77" stroke="white" strokeWidth="2.5" strokeLinecap="round"/>
            </svg>
            <h1 className="text-lg font-semibold text-white">
              Retrieval Studio
            </h1>
          </div>
        </div>

        {/* Project Selector */}
        <div className="p-4 border-b border-databricks-gray-200">
          {isLoading ? (
            <div className="flex items-center justify-center py-2">
              <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-databricks-blue"></div>
            </div>
          ) : projects.length > 0 ? (
            <Select
              value={selectedProjectId}
              onChange={(e) => setSelectedProjectId(e.target.value)}
              options={[
                { value: '', label: 'Select project' },
                ...projectOptions
              ]}
            />
          ) : (
            <p className="text-sm text-databricks-gray-500 text-center py-2">
              No projects yet
            </p>
          )}
        </div>

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto custom-scrollbar p-2">
          {menuItems.map((item) => {
            const Icon = item.icon
            const isActive = location.pathname === item.path

            return (
              <button
                key={item.path}
                onClick={() => navigate(item.path)}
                className={`w-full flex items-center px-4 py-2.5 mb-1 rounded-md text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-databricks-blue text-white'
                    : 'text-databricks-gray-700 hover:bg-databricks-gray-100'
                }`}
              >
                <Icon className="w-5 h-5 mr-3" />
                <span className="flex-1 text-left">{item.text}</span>
              </button>
            )
          })}
        </nav>

        {/* Footer/Version */}
        <div className="p-4 border-t border-databricks-gray-200">
          <p className="text-xs text-databricks-gray-500 text-center">
            Retrieval Studio v1.0.0
          </p>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top Bar with Breadcrumbs */}
        <div className="h-16 bg-white border-b border-databricks-gray-200 flex items-center px-8">
          <div className="flex items-center text-sm text-databricks-gray-600">
            <span className="text-databricks-blue cursor-pointer hover:underline">
              Retrieval Studio
            </span>
            <ChevronRight className="w-4 h-4 mx-2" />
            <span className="text-databricks-gray-900 font-medium">
              {menuItems.find(item => item.path === location.pathname)?.text || 'Dashboard'}
            </span>
          </div>
        </div>

        {/* Main Content Area */}
        <main className="flex-1 overflow-y-auto custom-scrollbar">
          <div className="max-w-7xl mx-auto p-8">
            {children}
          </div>
        </main>
      </div>
    </div>
  )
}
