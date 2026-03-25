-- Retrieval Studio - Lakehouse Postgres Schema
-- This schema stores application state for transactional workloads

-- Projects table
CREATE TABLE IF NOT EXISTS projects (
    project_id VARCHAR(50) PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    description TEXT,
    catalog VARCHAR(100),
    db_schema VARCHAR(100),
    vs_endpoint_name VARCHAR(255),
    embedding_model_endpoint VARCHAR(255),
    experiment_id VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_projects_created_at ON projects(created_at DESC);

-- Builds table (tracks build jobs)
CREATE TABLE IF NOT EXISTS builds (
    run_id VARCHAR(50) PRIMARY KEY,
    project_id VARCHAR(50) NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    project_name VARCHAR(255) NOT NULL,
    experiment_id VARCHAR(100),
    build_parent_run_id VARCHAR(50),
    state VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    job_id BIGINT,
    job_run_id BIGINT,
    eval_job_run_id BIGINT,
    job_url TEXT,
    config JSONB,
    results JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_builds_project_id ON builds(project_id);
CREATE INDEX idx_builds_state ON builds(state);
CREATE INDEX idx_builds_created_at ON builds(created_at DESC);
CREATE INDEX idx_builds_project_created ON builds(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_builds_experiment_id ON builds(experiment_id);

-- Evaluations table (tracks evaluation jobs)
CREATE TABLE IF NOT EXISTS evaluations (
    eval_id VARCHAR(50) PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL REFERENCES builds(run_id) ON DELETE CASCADE,
    project_id VARCHAR(50) NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    state VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    job_id BIGINT,
    job_run_id BIGINT,
    job_url TEXT,
    queries_table VARCHAR(255),
    corpus_table VARCHAR(255),
    dataset_type VARCHAR(20),
    top_k INTEGER DEFAULT 10,
    auto_generate_queries BOOLEAN DEFAULT FALSE,
    num_queries INTEGER,
    query_style VARCHAR(20),
    compare_query_types BOOLEAN DEFAULT FALSE,
    judge_model_endpoint VARCHAR(255),
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_evaluations_run_id ON evaluations(run_id);
CREATE INDEX idx_evaluations_project_id ON evaluations(project_id);
CREATE INDEX idx_evaluations_state ON evaluations(state);
CREATE INDEX idx_evaluations_created_at ON evaluations(created_at DESC);

-- Job runs tracking (detailed status for polling)
CREATE TABLE IF NOT EXISTS job_runs (
    job_run_id BIGINT PRIMARY KEY,
    run_id VARCHAR(50),
    job_type VARCHAR(20) NOT NULL, -- 'build' or 'eval'
    state VARCHAR(20) NOT NULL,
    result_state VARCHAR(20),
    job_url TEXT,
    start_time BIGINT,
    end_time BIGINT,
    setup_duration BIGINT,
    execution_duration BIGINT,
    cleanup_duration BIGINT,
    last_checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_runs_run_id ON job_runs(run_id);
CREATE INDEX idx_job_runs_state ON job_runs(state);
CREATE INDEX idx_job_runs_last_checked ON job_runs(last_checked_at DESC);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for auto-updating updated_at
CREATE TRIGGER update_projects_updated_at BEFORE UPDATE ON projects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_builds_updated_at BEFORE UPDATE ON builds
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_evaluations_updated_at BEFORE UPDATE ON evaluations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Index selections table (tracks VS indexes and delta tables created by builds)
CREATE TABLE IF NOT EXISTS index_selections (
    id VARCHAR(50) PRIMARY KEY,
    project_id VARCHAR(50) REFERENCES projects(project_id) ON DELETE CASCADE,
    build_run_id VARCHAR(50),
    source_name VARCHAR(255),
    strategy_name VARCHAR(255),
    index_name VARCHAR(500),
    chunks_table VARCHAR(500),
    vs_endpoint VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_index_selections_project ON index_selections(project_id);
CREATE INDEX IF NOT EXISTS idx_index_selections_status ON index_selections(status);

CREATE TRIGGER update_index_selections_updated_at BEFORE UPDATE ON index_selections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Studies table (groups builds and evaluations within a project)
CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR(50) PRIMARY KEY,
    project_id VARCHAR(50) REFERENCES projects(project_id) ON DELETE CASCADE,
    study_name VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_studies_project ON studies(project_id);

CREATE TRIGGER update_studies_updated_at BEFORE UPDATE ON studies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Study-build association table
CREATE TABLE IF NOT EXISTS study_builds (
    study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
    build_run_id VARCHAR(50) REFERENCES builds(run_id) ON DELETE CASCADE,
    PRIMARY KEY (study_id, build_run_id)
);

-- Study-evaluation association table
CREATE TABLE IF NOT EXISTS study_evaluations (
    study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
    eval_id VARCHAR(50) REFERENCES evaluations(eval_id) ON DELETE CASCADE,
    PRIMARY KEY (study_id, eval_id)
);

-- Comments for documentation
COMMENT ON TABLE projects IS 'Stores project metadata and configuration';
COMMENT ON TABLE builds IS 'Tracks build job submissions and status';
COMMENT ON TABLE evaluations IS 'Tracks evaluation job submissions and status';
COMMENT ON TABLE job_runs IS 'Detailed job execution tracking for status polling';
COMMENT ON TABLE index_selections IS 'Tracks VS indexes and delta tables for lifecycle management';
COMMENT ON TABLE studies IS 'Groups related builds and evaluations within a project';

COMMENT ON COLUMN builds.config IS 'JSONB field storing build configuration (sources, strategies, etc)';
COMMENT ON COLUMN builds.experiment_id IS 'MLflow experiment ID where runs for this build are stored';
COMMENT ON COLUMN evaluations.auto_generate_queries IS 'Whether queries were auto-generated from corpus';
COMMENT ON COLUMN evaluations.compare_query_types IS 'Whether to compare FULL_TEXT, ANN, HYBRID';
