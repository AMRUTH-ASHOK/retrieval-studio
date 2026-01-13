import { Chip } from '@mui/material'

interface StatusBadgeProps {
  status: string
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const getColor = () => {
    switch (status.toUpperCase()) {
      case 'SUCCESS':
        return 'success'
      case 'RUNNING':
        return 'primary'
      case 'FAILED':
        return 'error'
      case 'PENDING':
        return 'default'
      default:
        return 'default'
    }
  }

  return <Chip label={status} color={getColor()} size="small" />
}
