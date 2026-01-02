import { ReactNode } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import {
  Box,
  Drawer,
  AppBar,
  Toolbar,
  List,
  Typography,
  Divider,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
  CircularProgress,
} from '@mui/material'
import {
  Folder as FolderIcon,
  Build as BuildIcon,
  Assessment as AssessmentIcon,
  RateReview as ReviewIcon,
  Leaderboard as LeaderboardIcon,
} from '@mui/icons-material'
import { useProject } from '../context/ProjectContext'

const drawerWidth = 280

interface LayoutProps {
  children: ReactNode
}

export default function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { projects, selectedProjectId, setSelectedProjectId, isLoading } = useProject()

  const handleProjectChange = (event: SelectChangeEvent) => {
    setSelectedProjectId(event.target.value)
  }

  const menuItems = [
    { text: 'Project Setup', icon: <FolderIcon />, path: '/projects' },
    { text: 'Build', icon: <BuildIcon />, path: '/build' },
    { text: 'Evaluate', icon: <AssessmentIcon />, path: '/evaluate' },
    { text: 'Review', icon: <ReviewIcon />, path: '/review' },
    { text: 'Leaderboard', icon: <LeaderboardIcon />, path: '/leaderboard' },
  ]

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
        <Toolbar>
          <Typography variant="h6" noWrap component="div">
            🔍 Retrieval Studio
          </Typography>
        </Toolbar>
      </AppBar>
      <Drawer
        variant="permanent"
        sx={{
          width: drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
          },
        }}
      >
        <Toolbar />
        <Box sx={{ overflow: 'auto', p: 2 }}>
          {isLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 2 }}>
              <CircularProgress size={24} />
            </Box>
          ) : projects.length > 0 ? (
            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel>Project</InputLabel>
              <Select
                value={selectedProjectId}
                label="Project"
                onChange={handleProjectChange}
              >
                {projects.map((project) => (
                  <MenuItem key={project.project_id} value={project.project_id}>
                    {project.project_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          ) : (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              No projects yet. Create one first!
            </Typography>
          )}
          <Divider sx={{ mb: 2 }} />
          <List>
            {menuItems.map((item) => (
              <ListItem key={item.text} disablePadding>
                <ListItemButton
                  selected={location.pathname === item.path}
                  onClick={() => navigate(item.path)}
                >
                  <ListItemIcon>{item.icon}</ListItemIcon>
                  <ListItemText primary={item.text} />
                </ListItemButton>
              </ListItem>
            ))}
          </List>
        </Box>
      </Drawer>
      <Box component="main" sx={{ flexGrow: 1, p: 3 }}>
        <Toolbar />
        {children}
      </Box>
    </Box>
  )
}
