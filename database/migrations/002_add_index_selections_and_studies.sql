-- Migration 002: Add index_selections and studies tables
-- For per-source strategy builds and resource lifecycle management

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

DO $$ BEGIN
    CREATE TRIGGER update_index_selections_updated_at BEFORE UPDATE ON index_selections
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

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

DO $$ BEGIN
    CREATE TRIGGER update_studies_updated_at BEFORE UPDATE ON studies
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS study_builds (
    study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
    build_run_id VARCHAR(50) REFERENCES builds(run_id) ON DELETE CASCADE,
    PRIMARY KEY (study_id, build_run_id)
);

CREATE TABLE IF NOT EXISTS study_evaluations (
    study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
    eval_id VARCHAR(50) REFERENCES evaluations(eval_id) ON DELETE CASCADE,
    PRIMARY KEY (study_id, eval_id)
);
