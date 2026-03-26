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

const drawerWidth = 260

interface LayoutProps {
  children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { selectedProject } = useProject()

  const menuItems = [
    { text: 'Projects', icon: FolderOpen, path: '/projects' },
    { text: 'Build', icon: Hammer, path: '/build' },
    { text: 'Evaluate', icon: FlaskConical, path: '/evaluate' },
    { text: 'Review', icon: FileText, path: '/review' },
  ]

  const getPageTitle = (): string => {
    const match = menuItems.find(item => item.path === location.pathname)
    if (match) return match.text
    if (location.pathname.startsWith('/projects/')) return 'Project Details'
    return 'Dashboard'
  }

  return (
    <div className="flex h-screen bg-databricks-gray-50">
      {/* Sidebar */}
      <div 
        className="bg-white border-r border-databricks-gray-200 flex flex-col"
        style={{ width: drawerWidth }}
      >
        {/* Logo/Header */}
        <div className="h-16 flex items-center px-6 border-b border-databricks-gray-200">
          <h1 className="text-lg font-semibold text-databricks-gray-900">
            Retrieval Studio
          </h1>
        </div>

        {/* Active Project Indicator */}
        {selectedProject && (
          <div className="px-4 py-3 border-b border-databricks-gray-200 bg-blue-50/50">
            <p className="text-[10px] font-medium text-databricks-gray-500 uppercase tracking-wider">Active Project</p>
            <p className="text-sm font-semibold text-databricks-blue truncate mt-0.5">{selectedProject.project_name}</p>
          </div>
        )}

        {/* Navigation */}
        <nav className="flex-1 overflow-y-auto custom-scrollbar p-2">
          {menuItems.map((item) => {
            const Icon = item.icon
            const isActive = location.pathname === item.path || (item.path === '/projects' && location.pathname.startsWith('/projects/'))

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
            <span className="text-databricks-blue cursor-pointer hover:underline" onClick={() => navigate('/projects')}>
              Retrieval Studio
            </span>
            <ChevronRight className="w-4 h-4 mx-2" />
            <span className="text-databricks-gray-900 font-medium">
              {getPageTitle()}
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
