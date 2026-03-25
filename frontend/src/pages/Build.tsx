import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { PlayCircle, ChevronRight, ChevronLeft, CheckCircle2, ExternalLink, Clock, Copy, CheckCircle, Plus, Trash2, Upload, File, X, AlertCircle, Loader2 } from 'lucide-react'
import { buildsApi } from '../services/builds'
import { metadataApi } from '../services/metadata'
import { uploadsApi, UploadResponse } from '../services/uploads'
import { DataType, Strategy } from '../types'
import { useProject } from '../context/ProjectContext'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Select } from '../components/ui/Select'
import { Card, CardContent } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { useNavigate } from 'react-router-dom'

interface UploadedFileState {
  file: File
  status: 'pending' | 'uploading' | 'uploaded' | 'error'
  error?: string
}

interface DataSourceEntry {
  id: string
  name: string
  dataType: string
  config: Record<string, any>
  strategies: string[]
  files: UploadedFileState[]
  uploadedVolumePath: string | null
}

const FILE_UPLOAD_TYPES = ['csv', 'json', 'pdf', 'docx']

export default function Build() {
  const navigate = useNavigate()
  const { selectedProject, selectedProjectId } = useProject()
  const [activeStep, setActiveStep] = useState(0)
  const [dataTypes, setDataTypes] = useState<DataType[]>([])
  const [allStrategies, setAllStrategies] = useState<Strategy[]>([])
  const [embeddingEndpoint, setEmbeddingEndpoint] = useState('')
  const [vsEndpoint, setVsEndpoint] = useState('')
  const [dataSources, setDataSources] = useState<DataSourceEntry[]>([
    { id: crypto.randomUUID(), name: '', dataType: '', config: {}, strategies: [], files: [], uploadedVolumePath: null }
  ])
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [error, setError] = useState('')
  const [submittedRunId, setSubmittedRunId] = useState<string | null>(null)
  const [jobStatus, setJobStatus] = useState<{
    state: string; job_url: string | null; start_time: number | null; status: any; run_id: string
  } | null>(null)
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null)

  const steps = [
    { label: 'Add Data Sources', number: 1 },
    { label: 'Assign Strategies', number: 2 },
    { label: 'Configure Endpoints', number: 3 },
    { label: 'Review & Submit', number: 4 },
  ]

  useEffect(() => {
    loadMetadata()
    return () => { if (pollingIntervalRef.current) clearInterval(pollingIntervalRef.current) }
  }, [])

  useEffect(() => {
    if (submittedRunId) {
      pollJobStatus()
      pollingIntervalRef.current = setInterval(pollJobStatus, 5000)
    }
    return () => { if (pollingIntervalRef.current) { clearInterval(pollingIntervalRef.current); pollingIntervalRef.current = null } }
  }, [submittedRunId])

  const loadMetadata = async () => {
    try {
      const [dt, st] = await Promise.all([metadataApi.getDataTypes(), metadataApi.getStrategies()])
      setDataTypes(dt)
      setAllStrategies(st)
    } catch { setError('Failed to load metadata') }
  }

  const pollJobStatus = async () => {
    if (!submittedRunId) return
    try {
      const status = await buildsApi.getStatus(submittedRunId)
      setJobStatus({ state: status.state, job_url: status.job_url, start_time: status.start_time, status: status.status, run_id: status.run_id })
      if (status.state === 'SUCCESS') {
        if (pollingIntervalRef.current) { clearInterval(pollingIntervalRef.current); pollingIntervalRef.current = null }
        setTimeout(() => navigate('/evaluate'), 2000)
      } else if (status.state === 'FAILED') {
        if (pollingIntervalRef.current) { clearInterval(pollingIntervalRef.current); pollingIntervalRef.current = null }
      }
    } catch (e) { console.error('Failed to poll:', e) }
  }

  const addDataSource = () => {
    setDataSources(prev => [...prev, { id: crypto.randomUUID(), name: '', dataType: '', config: {}, strategies: [], files: [], uploadedVolumePath: null }])
  }

  const removeDataSource = (id: string) => {
    setDataSources(prev => prev.filter(s => s.id !== id))
  }

  const updateSource = (id: string, updates: Partial<DataSourceEntry>) => {
    setDataSources(prev => prev.map(s => s.id === id ? { ...s, ...updates } : s))
  }

  const updateSourceConfig = (id: string, field: string, value: any) => {
    setDataSources(prev => prev.map(s => s.id === id ? { ...s, config: { ...s.config, [field]: value } } : s))
  }

  const toggleStrategy = (sourceId: string, strategyName: string) => {
    setDataSources(prev => prev.map(s => {
      if (s.id !== sourceId) return s
      const strategies = s.strategies.includes(strategyName)
        ? s.strategies.filter(st => st !== strategyName)
        : [...s.strategies, strategyName]
      return { ...s, strategies }
    }))
  }

  const addFiles = (sourceId: string, fileList: FileList | null) => {
    if (!fileList) return
    const newFiles: UploadedFileState[] = Array.from(fileList).map(f => ({ file: f, status: 'pending' as const }))
    setDataSources(prev => prev.map(s => s.id === sourceId ? { ...s, files: [...s.files, ...newFiles], uploadedVolumePath: null } : s))
  }

  const removeFile = (sourceId: string, fileIndex: number) => {
    setDataSources(prev => prev.map(s => s.id === sourceId ? { ...s, files: s.files.filter((_, i) => i !== fileIndex), uploadedVolumePath: null } : s))
  }

  const uploadSourceFiles = async (source: DataSourceEntry): Promise<string | null> => {
    if (!selectedProject || source.files.length === 0) return null
    if (source.uploadedVolumePath) return source.uploadedVolumePath
    const filesToUpload = source.files.filter(f => f.status === 'pending' || f.status === 'error').map(f => f.file)
    if (filesToUpload.length === 0 && source.uploadedVolumePath) return source.uploadedVolumePath
    const response: UploadResponse = await uploadsApi.uploadFiles(filesToUpload, selectedProject.project_name)
    setDataSources(prev => prev.map(s => s.id === source.id ? { ...s, files: s.files.map(f => ({ ...f, status: 'uploaded' as const })), uploadedVolumePath: response.volume_path } : s))
    return response.volume_path
  }

  const getCompatibleStrategies = (dataType: string): string[] => {
    const dt = dataTypes.find(d => d.name === dataType)
    return dt?.compatible_strategies || []
  }

  const totalCombos = useMemo(() => dataSources.reduce((acc, s) => acc + s.strategies.length, 0), [dataSources])

  const handleSubmit = async () => {
    if (!selectedProjectId) { setError('Please select a project first'); return }

    const validSources = dataSources.filter(s => s.name && s.dataType && s.strategies.length > 0)
    if (validSources.length === 0) { setError('Please configure at least one source with strategies'); return }

    const names = validSources.map(s => s.name)
    if (new Set(names).size !== names.length) { setError('Source names must be unique'); return }

    setIsSubmitting(true)
    setError('')

    try {
      const sourcesPayload = []
      for (const source of validSources) {
        let srcConfig = { ...source.config }
        if (FILE_UPLOAD_TYPES.includes(source.dataType)) {
          if (source.files.length === 0) { setError(`Source "${source.name}" has no files`); setIsSubmitting(false); return }
          const volumePath = await uploadSourceFiles(source)
          if (!volumePath) { setError(`Failed to upload files for "${source.name}"`); setIsSubmitting(false); return }
          srcConfig = { ...srcConfig, volume_path: volumePath, file_pattern: `*.${source.dataType}` }
        }
        sourcesPayload.push({
          source_name: source.name,
          source_type: source.dataType,
          config: srcConfig,
          strategies: source.strategies.reduce((acc: Record<string, any>, s: string) => { acc[s] = {}; return acc }, {})
        })
      }

      const config = {
        sources: sourcesPayload,
        embedding_model_endpoint: embeddingEndpoint,
        vs_endpoint_name: vsEndpoint,
        create_index: true,
      }

      const result = await buildsApi.create({ project_id: selectedProjectId, config })
      setSubmittedRunId(result.run_id)
      try {
        const status = await buildsApi.getStatus(result.run_id)
        setJobStatus({ state: status.state, job_url: status.job_url, start_time: status.start_time, status: status.status, run_id: status.run_id })
      } catch {
        setJobStatus({ state: result.state, job_url: result.job_url || null, start_time: new Date(result.created_at).getTime(), status: null, run_id: result.run_id })
      }
      setActiveStep(0)
      setDataSources([{ id: crypto.randomUUID(), name: '', dataType: '', config: {}, strategies: [], files: [], uploadedVolumePath: null }])
      setEmbeddingEndpoint('')
      setVsEndpoint('')
    } catch (e) {
      console.error('Failed to submit build job:', e)
      setError('Failed to submit build job')
      setIsSubmitting(false)
    }
  }

  const formatDuration = (startTime: number | null, endTime: number | null) => {
    if (!startTime) return 'N/A'
    const end = endTime || Date.now()
    const seconds = Math.floor((end - startTime) / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ${seconds % 60}s`
    const hours = Math.floor(minutes / 60)
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`
  }

  const formatDateTime = (timestamp: number | null) => {
    if (!timestamp) return 'N/A'
    return new Date(timestamp).toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' })
  }

  const copyToClipboard = (text: string) => { navigator.clipboard.writeText(text) }

  const renderStepContent = (step: number) => {
    switch (step) {
      case 0:
        return (
          <div className="space-y-4">
            <div className="flex items-center justify-between mb-2">
              <div>
                <h3 className="text-lg font-medium text-databricks-gray-900">Configure Data Sources</h3>
                <p className="text-sm text-databricks-gray-500 mt-1">Add one or more data sources. Each source will be chunked independently.</p>
              </div>
              <Badge variant="secondary">{dataSources.filter(s => s.dataType).length} source{dataSources.filter(s => s.dataType).length !== 1 ? 's' : ''}</Badge>
            </div>

            {dataSources.map((source, index) => {
              const dtInfo = dataTypes.find(d => d.name === source.dataType)
              const isFileType = FILE_UPLOAD_TYPES.includes(source.dataType)
              return (
                <Card key={source.id} className="p-4 border-2 border-databricks-gray-200">
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className="w-7 h-7 rounded-full bg-databricks-blue/10 text-databricks-blue flex items-center justify-center text-xs font-bold">{index + 1}</div>
                      <h4 className="text-md font-medium text-databricks-gray-800">{source.name || 'New Source'}</h4>
                    </div>
                    {dataSources.length > 1 && (
                      <Button variant="ghost" size="sm" onClick={() => removeDataSource(source.id)} className="text-databricks-error hover:bg-red-50">
                        <Trash2 className="w-4 h-4 mr-1" /> Remove
                      </Button>
                    )}
                  </div>
                  <div className="space-y-4">
                    <Input label="Source Name" placeholder="e.g., clinical_pdfs" required value={source.name}
                      onChange={(e) => updateSource(source.id, { name: e.target.value.replace(/[^a-zA-Z0-9_-]/g, '_') })}
                      helperText="Alphanumeric with underscores/hyphens. Used in table and index names." />

                    <Select label="Data Type" value={source.dataType} required
                      onChange={(e) => updateSource(source.id, { dataType: e.target.value, config: {}, files: [], uploadedVolumePath: null, strategies: [] })}
                      options={[{ value: '', label: 'Select a data type' }, ...dataTypes.map(dt => ({ value: dt.name, label: dt.display_name }))]} />

                    {dtInfo?.input_schema?.fields?.map((field: any) => {
                      if (field.type === 'textarea') {
                        return (
                          <div key={field.name}>
                            <label className="block text-sm font-medium text-databricks-gray-700 mb-1">{field.label}{field.required && <span className="text-databricks-error ml-1">*</span>}</label>
                            <textarea value={source.config[field.name] || ''} onChange={(e) => updateSourceConfig(source.id, field.name, e.target.value)}
                              placeholder={field.default || ''} required={field.required} rows={4}
                              className="w-full px-3 py-2 border border-databricks-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-blue focus:border-databricks-blue" />
                          </div>
                        )
                      } else if (field.type === 'bool') {
                        return (
                          <div key={field.name} className="flex items-center">
                            <input type="checkbox" checked={source.config[field.name] ?? field.default ?? false} onChange={(e) => updateSourceConfig(source.id, field.name, e.target.checked)}
                              className="h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue" />
                            <label className="ml-2 text-sm text-databricks-gray-700">{field.label}</label>
                          </div>
                        )
                      }
                      return <Input key={field.name} label={field.label} placeholder={field.default || ''} required={field.required}
                        onChange={(e) => updateSourceConfig(source.id, field.name, e.target.value)} value={source.config[field.name] || ''} />
                    })}

                    {isFileType && (
                      <div className="space-y-3 pt-2 border-t border-databricks-gray-100">
                        <label className="block text-sm font-medium text-databricks-gray-700">Upload {source.dataType.toUpperCase()} Files</label>
                        <div
                          onDrop={(e) => { e.preventDefault(); addFiles(source.id, e.dataTransfer.files) }}
                          onDragOver={(e) => e.preventDefault()}
                          onClick={() => { const input = document.createElement('input'); input.type = 'file'; input.multiple = true; input.accept = `.${source.dataType}`; input.onchange = () => addFiles(source.id, input.files); input.click() }}
                          className="border-2 border-dashed rounded-lg p-5 text-center cursor-pointer transition-all duration-200 border-databricks-gray-300 hover:border-databricks-blue hover:bg-blue-50">
                          <Upload className="w-7 h-7 mx-auto mb-2 text-databricks-gray-400" />
                          <p className="text-sm text-databricks-gray-600">Drop {source.dataType.toUpperCase()} files or click to browse</p>
                        </div>
                        {source.files.length > 0 && (
                          <div className="max-h-32 overflow-y-auto space-y-1">
                            {source.files.map((fs, fi) => (
                              <div key={fi} className="flex items-center justify-between p-2 bg-databricks-gray-50 rounded border border-databricks-gray-200 text-sm">
                                <div className="flex items-center gap-2 flex-1 min-w-0">
                                  <File className="w-3.5 h-3.5 text-databricks-gray-500 flex-shrink-0" />
                                  <span className="truncate text-databricks-gray-800">{fs.file.name}</span>
                                  <span className="text-xs text-databricks-gray-400 flex-shrink-0">{(fs.file.size / 1024).toFixed(0)} KB</span>
                                  {fs.status === 'uploaded' && <CheckCircle className="w-3.5 h-3.5 text-green-500 flex-shrink-0" />}
                                </div>
                                <button onClick={(e) => { e.stopPropagation(); removeFile(source.id, fi) }} className="ml-1 p-0.5 text-databricks-gray-400 hover:text-databricks-error"><X className="w-3.5 h-3.5" /></button>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </Card>
              )
            })}

            <Button variant="outline" onClick={addDataSource} className="w-full border-2 border-dashed border-databricks-gray-300 hover:border-databricks-blue hover:bg-blue-50">
              <Plus className="w-4 h-4 mr-2" /> Add Another Source
            </Button>
          </div>
        )

      case 1:
        return (
          <div className="space-y-4">
            <div className="flex items-center justify-between mb-2">
              <div>
                <h3 className="text-lg font-medium text-databricks-gray-900">Assign Strategies Per Source</h3>
                <p className="text-sm text-databricks-gray-500 mt-1">Select chunking strategies for each data source. Different sources can use different strategies.</p>
              </div>
              <Badge variant="info">{totalCombos} table{totalCombos !== 1 ? 's' : ''} / index{totalCombos !== 1 ? 'es' : ''}</Badge>
            </div>

            {dataSources.filter(s => s.dataType && s.name).map((source) => {
              const compatible = getCompatibleStrategies(source.dataType)
              const dtInfo = dataTypes.find(d => d.name === source.dataType)
              return (
                <Card key={source.id} className="p-4 border-2 border-databricks-gray-200">
                  <div className="flex items-center gap-3 mb-4">
                    <h4 className="text-md font-semibold text-databricks-gray-900">{source.name}</h4>
                    <Badge variant="secondary">{dtInfo?.display_name || source.dataType}</Badge>
                    {source.strategies.length > 0 && <Badge variant="success">{source.strategies.length} selected</Badge>}
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                    {allStrategies.filter(s => compatible.includes(s.name)).map((strategy) => (
                      <div key={strategy.name}
                        className={`cursor-pointer rounded-lg border-2 p-3 transition-all ${source.strategies.includes(strategy.name) ? 'ring-2 ring-databricks-blue bg-blue-50 border-databricks-blue' : 'border-databricks-gray-200 hover:shadow-db-md'}`}
                        onClick={() => toggleStrategy(source.id, strategy.name)}>
                        <div className="flex items-start">
                          <input type="checkbox" checked={source.strategies.includes(strategy.name)} onChange={() => toggleStrategy(source.id, strategy.name)}
                            className="mt-1 h-4 w-4 text-databricks-blue border-databricks-gray-300 rounded focus:ring-databricks-blue" />
                          <div className="ml-3 flex-1">
                            <label className="font-medium text-databricks-gray-900 cursor-pointer">{strategy.display_name}</label>
                            <p className="text-xs text-databricks-gray-600 mt-1">{strategy.description}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                  {compatible.length === 0 && (
                    <p className="text-sm text-yellow-800 bg-yellow-50 p-3 rounded-md">No compatible strategies for this data type.</p>
                  )}
                </Card>
              )
            })}

            {dataSources.filter(s => s.dataType && s.name).length === 0 && (
              <div className="text-center py-8 text-sm text-databricks-gray-500">Go back and configure at least one data source.</div>
            )}
          </div>
        )

      case 2:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">Configure Endpoints</h3>
            <Input label="Embedding Model Endpoint" value={embeddingEndpoint} onChange={(e) => setEmbeddingEndpoint(e.target.value)}
              helperText="Databricks Model Serving endpoint for embeddings" placeholder="e.g., databricks-bge-large-en" required />
            <Input label="Vector Search Endpoint" value={vsEndpoint} onChange={(e) => setVsEndpoint(e.target.value)}
              helperText="Databricks Vector Search endpoint name" placeholder="e.g., vs-endpoint-default" required />
          </div>
        )

      case 3: {
        const validSources = dataSources.filter(s => s.name && s.dataType && s.strategies.length > 0)
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-4">Review Build Configuration</h3>

            <div className="grid grid-cols-3 gap-4 mb-6">
              <Card className="p-4 text-center bg-blue-50 border-blue-200">
                <p className="text-2xl font-bold text-databricks-blue">{validSources.length}</p>
                <p className="text-xs text-databricks-gray-600">Data Sources</p>
              </Card>
              <Card className="p-4 text-center bg-green-50 border-green-200">
                <p className="text-2xl font-bold text-green-700">{totalCombos}</p>
                <p className="text-xs text-databricks-gray-600">Delta Tables</p>
              </Card>
              <Card className="p-4 text-center bg-purple-50 border-purple-200">
                <p className="text-2xl font-bold text-purple-700">{totalCombos}</p>
                <p className="text-xs text-databricks-gray-600">VS Indexes</p>
              </Card>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-databricks-gray-200">
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Source</th>
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Type</th>
                    <th className="text-left py-2 px-3 text-databricks-gray-700">Strategies</th>
                    <th className="text-center py-2 px-3 text-databricks-gray-700">Tables/Indexes</th>
                  </tr>
                </thead>
                <tbody>
                  {validSources.map(source => {
                    const dtInfo = dataTypes.find(d => d.name === source.dataType)
                    return (
                      <tr key={source.id} className="border-b border-databricks-gray-100">
                        <td className="py-2 px-3 font-medium text-databricks-gray-900">{source.name}</td>
                        <td className="py-2 px-3"><Badge variant="secondary">{dtInfo?.display_name || source.dataType}</Badge></td>
                        <td className="py-2 px-3">
                          <div className="flex flex-wrap gap-1">
                            {source.strategies.map(s => <Badge key={s} variant="info">{s}</Badge>)}
                          </div>
                        </td>
                        <td className="py-2 px-3 text-center font-semibold">{source.strategies.length}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>

            <div className="p-4 bg-databricks-gray-50 rounded-md border border-databricks-gray-200 mt-4">
              <p className="text-sm text-databricks-gray-600">
                <span className="font-medium">Embedding:</span> {embeddingEndpoint || 'Not set'} &nbsp;|&nbsp;
                <span className="font-medium">VS Endpoint:</span> {vsEndpoint || 'Not set'}
              </p>
            </div>
          </div>
        )
      }

      default: return null
    }
  }

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-databricks-gray-900">Build Retrieval Pipeline</h1>
        <p className="text-sm text-databricks-gray-600 mt-1">Configure per-source chunking strategies and create vector search indexes</p>
      </div>

      {!selectedProjectId ? (
        <Card>
          <div className="text-center py-12">
            <div className="w-16 h-16 mx-auto mb-4 bg-yellow-100 rounded-full flex items-center justify-center"><Clock className="w-8 h-8 text-yellow-600" /></div>
            <h3 className="text-lg font-medium text-databricks-gray-900 mb-2">No Project Selected</h3>
            <p className="text-sm text-databricks-gray-600 mb-6 max-w-md mx-auto">Please go to the Projects page and select a project to continue.</p>
            <Button variant="primary" onClick={() => navigate('/projects')}>Go to Projects</Button>
          </div>
        </Card>
      ) : (
        <>
          {selectedProject && (
            <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-md">
              <p className="text-sm text-blue-900"><span className="font-medium">Current project:</span> {selectedProject.project_name}</p>
            </div>
          )}

          <Card>
            <div className="mb-8">
              <div className="flex items-center justify-between">
                {steps.map((step, index) => (
                  <div key={step.number} className="flex items-center flex-1">
                    <div className="flex items-center">
                      <div className={`flex items-center justify-center w-10 h-10 rounded-full border-2 font-medium text-sm transition-colors ${index < activeStep ? 'bg-databricks-blue border-databricks-blue text-white' : index === activeStep ? 'border-databricks-blue text-databricks-blue bg-white' : 'border-databricks-gray-300 text-databricks-gray-500 bg-white'}`}>
                        {index < activeStep ? <CheckCircle2 className="w-5 h-5" /> : step.number}
                      </div>
                      <div className="ml-3"><p className={`text-sm font-medium ${index <= activeStep ? 'text-databricks-gray-900' : 'text-databricks-gray-500'}`}>{step.label}</p></div>
                    </div>
                    {index < steps.length - 1 && <div className={`flex-1 h-0.5 mx-4 ${index < activeStep ? 'bg-databricks-blue' : 'bg-databricks-gray-300'}`} />}
                  </div>
                ))}
              </div>
            </div>

            <div className="min-h-[300px] mb-8">{renderStepContent(activeStep)}</div>

            {error && (
              <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-md"><p className="text-sm text-red-800">{error}</p></div>
            )}

            <div className="flex justify-between items-center pt-6 border-t border-databricks-gray-200">
              <Button variant="outline" onClick={() => setActiveStep(prev => Math.max(prev - 1, 0))} disabled={activeStep === 0} icon={<ChevronLeft className="w-4 h-4" />}>Back</Button>
              <div className="flex gap-3">
                {activeStep === steps.length - 1 ? (
                  <Button variant="primary" onClick={handleSubmit} isLoading={isSubmitting} disabled={!selectedProjectId || isSubmitting || totalCombos === 0}
                    icon={<PlayCircle className="w-4 h-4" />}>Submit Build Job</Button>
                ) : (
                  <Button variant="primary" onClick={() => setActiveStep(prev => Math.min(prev + 1, steps.length - 1))} icon={<ChevronRight className="w-4 h-4" />}>Next</Button>
                )}
              </div>
            </div>
          </Card>

          {jobStatus && submittedRunId && (
            <Card className="mt-6">
              <CardContent className="p-6">
                <div className="flex items-start justify-between mb-6">
                  <h2 className="text-lg font-semibold text-databricks-gray-900">Build Job Status</h2>
                  <Badge variant={jobStatus.state === 'SUCCESS' ? 'success' : jobStatus.state === 'FAILED' ? 'error' : jobStatus.state === 'RUNNING' ? 'info' : 'warning'}>
                    {jobStatus.state === 'SUCCESS' && <CheckCircle className="w-3 h-3 mr-1" />}{jobStatus.state}
                  </Badge>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                  <div className="space-y-3">
                    <div>
                      <label className="text-xs font-medium text-databricks-gray-500 uppercase">Job ID</label>
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-sm font-mono text-databricks-gray-900">{jobStatus.status?.job_id || 'N/A'}</span>
                        {jobStatus.status?.job_id && <button onClick={() => copyToClipboard(String(jobStatus.status.job_id))} className="text-databricks-gray-400 hover:text-databricks-gray-600" title="Copy"><Copy className="w-3 h-3" /></button>}
                      </div>
                    </div>
                    <div>
                      <label className="text-xs font-medium text-databricks-gray-500 uppercase">Run ID</label>
                      <div className="flex items-center gap-2 mt-1">
                        <span className="text-sm font-mono text-databricks-gray-900">{jobStatus.status?.run_id || 'N/A'}</span>
                      </div>
                    </div>
                  </div>
                  <div className="space-y-3">
                    <div>
                      <label className="text-xs font-medium text-databricks-gray-500 uppercase">Started</label>
                      <p className="text-sm text-databricks-gray-900 mt-1">{formatDateTime(jobStatus.start_time)}</p>
                    </div>
                    <div>
                      <label className="text-xs font-medium text-databricks-gray-500 uppercase">Duration</label>
                      <p className="text-sm text-databricks-gray-900 mt-1">{formatDuration(jobStatus.start_time, jobStatus.status?.end_time)}</p>
                    </div>
                  </div>
                </div>
                {jobStatus.job_url && (
                  <div className="pt-4 border-t border-databricks-gray-200">
                    <a href={jobStatus.job_url} target="_blank" rel="noopener noreferrer" className="inline-flex items-center text-sm text-databricks-blue hover:underline">
                      <ExternalLink className="w-4 h-4 mr-1" /> View Job in Databricks
                    </a>
                  </div>
                )}
                {jobStatus.state === 'SUCCESS' && (
                  <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-md">
                    <p className="text-sm text-green-800">Build job completed successfully! Redirecting to Evaluate page...</p>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  )
}
