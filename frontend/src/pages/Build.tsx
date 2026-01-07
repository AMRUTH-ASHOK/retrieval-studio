import { useState, useEffect } from 'react'
import { PlayCircle, ChevronRight, ChevronLeft, CheckCircle2 } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { metadataApi } from '../services/metadata'
import { DataType, Strategy } from '../types'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card, CardContent } from '../components/ui/Card'

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
  const [error, setError] = useState('')

  const steps = [
    { label: 'Select Data Type', number: 1 },
    { label: 'Configure Data Source', number: 2 },
    { label: 'Select Strategies', number: 3 },
    { label: 'Configure & Run', number: 4 },
  ]

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
      setError('Failed to load metadata')
    }
  }

  const handleNext = () => {
    setActiveStep((prev) => Math.min(prev + 1, steps.length - 1))
  }

  const handleBack = () => {
    setActiveStep((prev) => Math.max(prev - 1, 0))
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
      setError('Please select a project first')
      return
    }

    setIsSubmitting(true)
    setError('')

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

      // Reset form
      setActiveStep(0)
      setSelectedDataType('')
      setSelectedStrategies([])
      setDataConfig({})
      setEmbeddingEndpoint('')
      setVsEndpoint('')
      
      alert('Build job submitted successfully!')
    } catch (error) {
      console.error('Failed to submit build job:', error)
      setError('Failed to submit build job')
    } finally {
      setIsSubmitting(false)
    }
  }

  const selectedDataTypeInfo = dataTypes.find((dt) => dt.name === selectedDataType)

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <div className="space-y-4">
            <Select
              label="Data Type"
              value={selectedDataType}
              onChange={(e) => setSelectedDataType(e.target.value)}
              options={[
                { value: '', label: 'Select a data type' },
                ...dataTypes.map((dt) => ({
                  value: dt.name,
                  label: dt.display_name,
                })),
              ]}
              required
            />
            {selectedDataTypeInfo && (
              <div className="p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200">
                <p className="text-sm text-databricks-gray-600">
                  <span className="font-medium">Compatible strategies:</span>{' '}
                  {selectedDataTypeInfo.compatible_strategies.join(', ')}
                </p>
              </div>
            )}
          </div>
        )

      case 1:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Configure Data Source
            </h3>
            {selectedDataTypeInfo?.input_schema?.fields?.map((field: any) => (
              <Input
                key={field.name}
                label={field.label}
                placeholder={field.default || ''}
                required={field.required}
                onChange={(e) =>
                  setDataConfig((prev) => ({ ...prev, [field.name]: e.target.value }))
                }
                value={dataConfig[field.name] || ''}
              />
            ))}
          </div>
        )

      case 2:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Select Chunking Strategies
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {strategies
                .filter(
                  (s) =>
                    !selectedDataTypeInfo ||
                    selectedDataTypeInfo.compatible_strategies.includes(s.name)
                )
                .map((strategy) => (
                  <Card
                    key={strategy.name}
                    className={`cursor-pointer transition-all ${
                      selectedStrategies.includes(strategy.name)
                        ? 'ring-2 ring-databricks-blue bg-blue-50'
                        : 'hover:shadow-db-md'
                    }`}
                    padding={false}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start">
                        <input
                          type="checkbox"
                          checked={selectedStrategies.includes(strategy.name)}
                          onChange={() => handleStrategyToggle(strategy.name)}
                          className="mt-1 h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue"
                        />
                        <div className="ml-3 flex-1">
                          <label className="font-medium text-databricks-gray-900 cursor-pointer">
                            {strategy.display_name}
                          </label>
                          <p className="text-sm text-databricks-gray-600 mt-1">
                            {strategy.description}
                          </p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
            </div>
          </div>
        )

      case 3:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">
              Configure Endpoints
            </h3>
            <Input
              label="Embedding Model Endpoint"
              value={embeddingEndpoint}
              onChange={(e) => setEmbeddingEndpoint(e.target.value)}
              helperText="Databricks Model Serving endpoint for embeddings"
              placeholder="e.g., databricks-bge-large-en"
              required
            />
            <Input
              label="Vector Search Endpoint"
              value={vsEndpoint}
              onChange={(e) => setVsEndpoint(e.target.value)}
              helperText="Databricks Vector Search endpoint name"
              placeholder="e.g., vs-endpoint-default"
              required
            />
          </div>
        )

      default:
        return null
    }
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Build Retrieval Pipeline</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">
          Configure and submit build jobs for your retrieval pipeline
        </p>
      </div>

      {!selectedProjectId && (
        <div className="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
          <p className="text-sm text-yellow-800">
            Please select a project from the sidebar to start building.
          </p>
        </div>
      )}

      {selectedProject && (
        <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
          <p className="text-sm text-blue-900">
            <span className="font-medium">Current project:</span> {selectedProject.project_name}
          </p>
        </div>
      )}

      <Card>
        {/* Progress Steps */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            {steps.map((step, index) => (
              <div key={step.number} className="flex items-center flex-1">
                <div className="flex items-center">
                  <div
                    className={`flex items-center justify-center w-10 h-10 rounded-full border-2 font-medium text-sm transition-colors ${
                      index < activeStep
                        ? 'bg-databricks-blue border-databricks-blue text-white'
                        : index === activeStep
                        ? 'border-databricks-blue text-databricks-blue bg-white'
                        : 'border-databricks-gray-300 text-databricks-gray-500 bg-white'
                    }`}
                  >
                    {index < activeStep ? (
                      <CheckCircle2 className="w-5 h-5" />
                    ) : (
                      step.number
                    )}
                  </div>
                  <div className="ml-3">
                    <p
                      className={`text-sm font-medium ${
                        index <= activeStep
                          ? 'text-databricks-gray-900'
                          : 'text-databricks-gray-500'
                      }`}
                    >
                      {step.label}
                    </p>
                  </div>
                </div>
                {index < steps.length - 1 && (
                  <div
                    className={`flex-1 h-0.5 mx-4 ${
                      index < activeStep ? 'bg-databricks-blue' : 'bg-databricks-gray-300'
                    }`}
                  />
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Step Content */}
        <div className="min-h-[300px] mb-8">{renderStepContent(activeStep)}</div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-md">
            <p className="text-sm text-red-800">{error}</p>
          </div>
        )}

        {/* Navigation Buttons */}
        <div className="flex justify-between items-center pt-6 border-t border-databricks-gray-200">
          <Button
            variant="outline"
            onClick={handleBack}
            disabled={activeStep === 0}
            icon={<ChevronLeft className="w-4 h-4" />}
          >
            Back
          </Button>

          <div className="flex gap-3">
            {activeStep === steps.length - 1 ? (
              <Button
                variant="primary"
                onClick={handleSubmit}
                isLoading={isSubmitting}
                disabled={!selectedProjectId || isSubmitting}
                icon={<PlayCircle className="w-4 h-4" />}
              >
                Submit Build Job
              </Button>
            ) : (
              <Button
                variant="primary"
                onClick={handleNext}
                icon={<ChevronRight className="w-4 h-4" />}
              >
                Next
              </Button>
            )}
          </div>
        </div>
      </Card>
    </div>
  )
}
