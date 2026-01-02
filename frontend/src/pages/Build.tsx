import { useState, useEffect } from 'react'
import {
  Box,
  Typography,
  Button,
  Paper,
  Stepper,
  Step,
  StepLabel,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Grid,
  Card,
  CardContent,
  Checkbox,
  FormControlLabel,
  Alert,
} from '@mui/material'
import { PlayArrow as PlayIcon } from '@mui/icons-material'
import { buildsApi } from '../services/builds'
import { metadataApi } from '../services/metadata'
import { DataType, Strategy } from '../types'
import { useProject } from '../context/ProjectContext'

export default function Build() {
  const { selectedProject, selectedProjectId } = useProject()
  const [activeStep, setActiveStep] = useState(0)
  const [dataTypes, setDataTypes] = useState<DataType[]>([])
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [selectedDataType, setSelectedDataType] = useState('')
  const [selectedStrategies, setSelectedStrategies] = useState<string[]>([])
  const [embeddingEndpoint, setEmbeddingEndpoint] = useState('')
  const [vsEndpoint, setVsEndpoint] = useState('')
  const [dataConfig, setDataConfig] = useState<Record<string, any>>({})
  const [isSubmitting, setIsSubmitting] = useState(false)

  const steps = ['Select Data Type', 'Configure Data Source', 'Select Strategies', 'Configure & Run']

  useEffect(() => {
    loadMetadata()
  }, [])

  const loadMetadata = async () => {
    try {
      const [dataTypesData, strategiesData] = await Promise.all([
        metadataApi.getDataTypes(),
        metadataApi.getStrategies(),
      ])
      setDataTypes(dataTypesData)
      setStrategies(strategiesData)
    } catch (error) {
      console.error('Failed to load metadata:', error)
    }
  }

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1)
  }

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1)
  }

  const handleStrategyToggle = (strategyName: string) => {
    setSelectedStrategies((prev) =>
      prev.includes(strategyName)
        ? prev.filter((s) => s !== strategyName)
        : [...prev, strategyName]
    )
  }

  const handleSubmit = async () => {
    if (!selectedProjectId) {
      alert('Please select a project first')
      return
    }

    setIsSubmitting(true)
    try {
      const config = {
        data_type: selectedDataType,
        data_config: dataConfig,
        strategies: selectedStrategies.reduce((acc, s) => {
          acc[s] = {}
          return acc
        }, {} as Record<string, any>),
        embedding_model_endpoint: embeddingEndpoint,
        vs_endpoint_name: vsEndpoint,
        create_index: true,
      }

      await buildsApi.create({
        project_id: selectedProjectId,
        config,
      })

      alert('Build job submitted successfully!')
      setActiveStep(0)
    } catch (error) {
      console.error('Failed to submit build job:', error)
      alert('Failed to submit build job')
    } finally {
      setIsSubmitting(false)
    }
  }

  const selectedDataTypeInfo = dataTypes.find(dt => dt.name === selectedDataType)

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <Box>
            <FormControl fullWidth>
              <InputLabel>Data Type</InputLabel>
              <Select
                value={selectedDataType}
                label="Data Type"
                onChange={(e) => setSelectedDataType(e.target.value)}
              >
                {dataTypes.map((dt) => (
                  <MenuItem key={dt.name} value={dt.name}>
                    {dt.display_name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            {selectedDataTypeInfo && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                Compatible strategies: {selectedDataTypeInfo.compatible_strategies.join(', ')}
              </Typography>
            )}
          </Box>
        )
      case 1:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Configure Data Source
            </Typography>
            {selectedDataTypeInfo?.input_schema?.fields?.map((field: any) => (
              <TextField
                key={field.name}
                fullWidth
                label={field.label}
                placeholder={field.default || ''}
                required={field.required}
                sx={{ mb: 2 }}
                onChange={(e) => setDataConfig(prev => ({ ...prev, [field.name]: e.target.value }))}
              />
            ))}
          </Box>
        )
      case 2:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Select Chunking Strategies
            </Typography>
            <Grid container spacing={2}>
              {strategies
                .filter(s => !selectedDataTypeInfo || selectedDataTypeInfo.compatible_strategies.includes(s.name))
                .map((strategy) => (
                <Grid item xs={12} sm={6} md={4} key={strategy.name}>
                  <Card sx={{ height: '100%' }}>
                    <CardContent>
                      <FormControlLabel
                        control={
                          <Checkbox
                            checked={selectedStrategies.includes(strategy.name)}
                            onChange={() => handleStrategyToggle(strategy.name)}
                          />
                        }
                        label={strategy.display_name}
                      />
                      <Typography variant="body2" color="text.secondary">
                        {strategy.description}
                      </Typography>
                    </CardContent>
                  </Card>
                </Grid>
              ))}
            </Grid>
          </Box>
        )
      case 3:
        return (
          <Box>
            <Typography variant="h6" gutterBottom>
              Configure Endpoints
            </Typography>
            <TextField
              fullWidth
              label="Embedding Model Endpoint"
              value={embeddingEndpoint}
              onChange={(e) => setEmbeddingEndpoint(e.target.value)}
              sx={{ mb: 2 }}
              helperText="Databricks Model Serving endpoint for embeddings"
            />
            <TextField
              fullWidth
              label="Vector Search Endpoint"
              value={vsEndpoint}
              onChange={(e) => setVsEndpoint(e.target.value)}
              helperText="Databricks Vector Search endpoint name"
            />
          </Box>
        )
      default:
        return null
    }
  }

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Build Retrieval Pipeline
      </Typography>

      {!selectedProjectId && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Please select a project from the sidebar to start building.
        </Alert>
      )}

      {selectedProject && (
        <Typography variant="subtitle1" color="text.secondary" gutterBottom>
          Project: {selectedProject.project_name}
        </Typography>
      )}

      <Paper sx={{ p: 3, mt: 3 }}>
        <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
          {steps.map((label) => (
            <Step key={label}>
              <StepLabel>{label}</StepLabel>
            </Step>
          ))}
        </Stepper>

        <Box sx={{ minHeight: 300, mb: 3 }}>{renderStepContent(activeStep)}</Box>

        <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
          <Button disabled={activeStep === 0} onClick={handleBack}>
            Back
          </Button>
          <Box>
            {activeStep === steps.length - 1 ? (
              <Button 
                variant="contained" 
                onClick={handleSubmit} 
                startIcon={<PlayIcon />}
                disabled={!selectedProjectId || isSubmitting}
              >
                {isSubmitting ? 'Submitting...' : 'Submit Build Job'}
              </Button>
            ) : (
              <Button variant="contained" onClick={handleNext}>
                Next
              </Button>
            )}
          </Box>
        </Box>
      </Paper>
    </Box>
  )
}
