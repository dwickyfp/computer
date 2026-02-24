-- ETL Stream Configuration Database Schema

-- Table 1: Sources (PostgreSQL connection configurations)
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    pg_host VARCHAR(255) NOT NULL,
    pg_port INTEGER NOT NULL DEFAULT 5432,
    pg_database VARCHAR(255) NOT NULL,
    pg_username VARCHAR(255) NOT NULL,
    pg_password VARCHAR(255),
    publication_name VARCHAR(255) NOT NULL,
    replication_name VARCHAR(255) NOT NULL,
    is_publication_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    is_replication_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    last_check_replication_publication TIMESTAMPTZ NULL,
    total_tables INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Drop constraint unique
ALTER TABLE sources DROP CONSTRAINT IF EXISTS unique_replication_name;
ALTER TABLE sources DROP CONSTRAINT IF EXISTS unique_publication_name;



-- Table 2: Destinations Snowflake
CREATE TABLE IF NOT EXISTS destinations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    type VARCHAR(50) NOT NULL DEFAULT 'SNOWFLAKE',
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 3: Pipelines (connects source to destination)
CREATE TABLE IF NOT EXISTS pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,-- 'SNOWFLAKE' or 'POSTGRESQL'
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'PAUSE', -- 'START' or 'PAUSE' or 'REFRESH
    ready_refresh BOOLEAN NOT NULL DEFAULT FALSE,
    last_refresh_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Alter table pipelines add column ready_refresh if not exists
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS ready_refresh BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS last_refresh_at TIMESTAMPTZ NULL;

-- 1 pipelines sources, now can have more then 1 destination
CREATE TABLE IF NOT EXISTS pipelines_destination (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    last_error_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table Metadata Sync Postgres to Postgres
CREATE TABLE IF NOT EXISTS pipelines_destination_table_sync(
    id SERIAL PRIMARY KEY,
    pipeline_destination_id INTEGER NOT NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_name_target VARCHAR(255) NOT NULL,
    custom_sql TEXT NULL,
    filter_sql TEXT NULL,
    primary_key_column_target TEXT NULL,
    is_exists_table_landing BOOLEAN DEFAULT FALSE, -- table landing in snowflake
    is_exists_stream BOOLEAN DEFAULT FALSE, -- stream in snowflake
    is_exists_task BOOLEAN DEFAULT FALSE, -- task in snowflake
    is_exists_table_destination BOOLEAN DEFAULT FALSE, -- table destination in snowflake
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    lineage_metadata JSONB NULL,
    lineage_status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, GENERATING, COMPLETED, FAILED
    lineage_error TEXT NULL,
    lineage_generated_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Alter table pipelines_destination_table_sync add primary_key_column_target if not exists
ALTER TABLE pipelines_destination_table_sync ADD COLUMN IF NOT EXISTS primary_key_column_target TEXT NULL; 

-- Add lineage_metadata column for storing column-level data lineage
ALTER TABLE pipelines_destination_table_sync ADD COLUMN IF NOT EXISTS lineage_metadata JSONB NULL;
ALTER TABLE pipelines_destination_table_sync ADD COLUMN IF NOT EXISTS lineage_status VARCHAR(20) DEFAULT 'PENDING'; -- PENDING, GENERATING, COMPLETED, FAILED
ALTER TABLE pipelines_destination_table_sync ADD COLUMN IF NOT EXISTS lineage_error TEXT NULL;
ALTER TABLE pipelines_destination_table_sync ADD COLUMN IF NOT EXISTS lineage_generated_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_lineage_status ON pipelines_destination_table_sync(lineage_status);

COMMENT ON COLUMN pipelines_destination_table_sync.lineage_metadata IS 'JSON containing column-level lineage: {source_tables, source_columns, output_columns, column_lineage}';

-- Table 4: Pipeline Metadata (contains runtime information)
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- 'RUNNING' or 'PAUSED'
    last_error TEXT NULL,
    last_error_at TIMESTAMPTZ NULL,
    last_start_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    cpu_usage FLOAT4,        -- Percentage
    total_memory BIGINT,     -- In KB
    used_memory BIGINT,      -- In KB
    total_swap BIGINT,       -- In KB
    used_swap BIGINT,        -- In KB
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 5: WAL Monitor (tracks Write-Ahead Log status per source)
CREATE TABLE IF NOT EXISTS wal_monitor (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    wal_lsn VARCHAR(255),           -- Log Sequence Number (e.g., '0/1234ABCD')
    wal_position BIGINT,            -- WAL position as numeric value
    last_wal_received TIMESTAMPTZ,   -- Last time WAL data was received
    last_transaction_time TIMESTAMPTZ, -- Last transaction timestamp
    replication_slot_name VARCHAR(255), -- Name of the replication slot
    replication_lag_bytes BIGINT,   -- Replication lag in bytes
    total_wal_size VARCHAR(255),    -- Total size of WAL files (e.g., '640 MB')
    status VARCHAR(20) DEFAULT 'ACTIVE', -- 'ACTIVE', 'IDLE', 'ERROR'
    error_message TEXT,             -- Error details if any
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_source_wal UNIQUE (source_id) -- Ensures 1 source = 1 row
);

-- Table 5A: WAL Metrics (stores historical WAL size data)
CREATE TABLE IF NOT EXISTS wal_metrics (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    size_bytes BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wal_metrics_source_id ON wal_metrics(source_id);
CREATE INDEX IF NOT EXISTS idx_wal_metrics_recorded_at ON wal_metrics(recorded_at);

-- Save lisat table based on publication, schema table, check table name
CREATE TABLE IF NOT EXISTS table_metadata_list (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255),
    schema_table JSONB NULL,
    is_changes_schema BOOLEAN DEFAULT FALSE, -- track changes schema
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- track schema changes based on table in table_metadata_list (Append Only)
CREATE TABLE IF NOT EXISTS history_schema_evolution (
    id SERIAL PRIMARY KEY,
    table_metadata_list_id INTEGER NOT NULL REFERENCES table_metadata_list(id) ON DELETE CASCADE,
    schema_table_old JSONB NULL,
    schema_table_new JSONB NULL,
    changes_type VARCHAR(20) NULL, -- 'NEW COLUMN', 'DROP COLUMN', 'CHANGES TYPE', 
    version_schema INTEGER NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

DELETE FROM history_schema_evolution a
USING history_schema_evolution b
WHERE a.id > b.id
  AND a.table_metadata_list_id = b.table_metadata_list_id
  AND a.version_schema = b.version_schema;

-- Drop Constraint if exists
ALTER TABLE history_schema_evolution DROP CONSTRAINT IF EXISTS uq_history_schema_table_version;

ALTER TABLE history_schema_evolution
ADD CONSTRAINT uq_history_schema_table_version 
UNIQUE (table_metadata_list_id, version_schema);

CREATE TABLE IF NOT EXISTS presets (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    table_names TEXT[] NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline_id ON pipeline_metadata(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status ON pipeline_metadata(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_source_id ON wal_monitor(source_id);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_status ON wal_monitor(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_last_received ON wal_monitor(last_wal_received);
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_source_id ON table_metadata_list(source_id);
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_table_name ON table_metadata_list(table_name);
CREATE INDEX IF NOT EXISTS idx_history_schema_evolution_table_metadata_list_id ON history_schema_evolution(table_metadata_list_id);
CREATE INDEX IF NOT EXISTS idx_history_schema_evolution_version_schema ON history_schema_evolution(version_schema);

-- Table 6: Pipeline Progress (tracks initialization progress)
CREATE TABLE IF NOT EXISTS pipelines_progress (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    progress INTEGER NOT NULL DEFAULT 0, -- 0 to 100
    step VARCHAR(255), -- current step description e.g. "Creating Landing Table"
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'
    details TEXT, -- JSON or text details about the progress
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipelines_progress_pipeline_id ON pipelines_progress(pipeline_id);

CREATE TABLE IF NOT EXISTS credit_snowflake_monitoring(
    id SERIAL PRIMARY KEY,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    total_credit NUMERIC(38, 9) NOT NULL,
    usage_date TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_credit_snowflake_monitoring_destination_id ON credit_snowflake_monitoring(destination_id);

-- Goals is to track record count of each table in each pipeline
CREATE TABLE IF NOT EXISTS data_flow_record_monitoring(
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    pipeline_destination_id INTEGER NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE,
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    pipeline_destination_table_sync_id INTEGER NOT NULL REFERENCES pipelines_destination_table_sync(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    record_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure columns exist (for schema evolution on existing tables)
ALTER TABLE data_flow_record_monitoring ADD COLUMN IF NOT EXISTS pipeline_destination_id INTEGER NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE;
ALTER TABLE data_flow_record_monitoring ADD COLUMN IF NOT EXISTS pipeline_destination_table_sync_id INTEGER NULL REFERENCES pipelines_destination_table_sync(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_pipeline_id ON data_flow_record_monitoring(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_pipeline_destination_id ON data_flow_record_monitoring(pipeline_destination_id);


CREATE TABLE IF NOT EXISTS rosetta_setting_configuration(
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(255) NOT NULL UNIQUE,
    config_value VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('WAL_MONITORING_THRESHOLD_WARNING', '3000') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('WAL_MONITORING_THRESHOLD_ERROR', '6000') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ENABLE_ALERT_NOTIFICATION_WEBHOOK', 'FALSE') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ALERT_NOTIFICATION_WEBHOOK_URL', '') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('NOTIFICATION_ITERATION_DEFAULT', '3') ON CONFLICT(config_key) DO NOTHING;

-- SETTING FOR BATCH 
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('PIPELINE_MAX_BATCH_SIZE', '4096') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('PIPELINE_MAX_QUEUE_SIZE', '16384') ON CONFLICT(config_key) DO NOTHING;

-- NOTIFICATION VIA TELEGRAM BOT
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ENABLE_ALERT_NOTIFICATION_TELEGRAM', 'FALSE') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ALERT_NOTIFICATION_TELEGRAM_KEY', '') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ALERT_NOTIFICATION_TELEGRAM_GROUP_ID', 'FALSE') ON CONFLICT(config_key) DO NOTHING;

-- NEW INDEX
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_source_table ON table_metadata_list(source_id, table_name);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_created_at ON data_flow_record_monitoring(created_at);
CREATE INDEX IF NOT EXISTS idx_credit_snowflake_monitoring_usage_date ON credit_snowflake_monitoring(usage_date);

-- Add unique constraint to table_metadata_list (Added retroactively for new deployments)
ALTER TABLE table_metadata_list DROP CONSTRAINT IF EXISTS uq_table_metadata_source_table;
ALTER TABLE table_metadata_list ADD CONSTRAINT uq_table_metadata_source_table UNIQUE (source_id, table_name);

CREATE TABLE IF NOT EXISTS job_metrics_monitoring(
    id SERIAL PRIMARY KEY,
    key_job_scheduler VARCHAR(255) NOT NULL ,
    last_run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE job_metrics_monitoring DROP CONSTRAINT IF EXISTS uq_job_metrics_monitoring_key_job_scheduler;
ALTER TABLE job_metrics_monitoring ADD CONSTRAINT uq_job_metrics_monitoring_key_job_scheduler UNIQUE (key_job_scheduler);

-- Constraint for ON CONFLICT support
ALTER TABLE pipeline_metadata DROP CONSTRAINT IF EXISTS uq_pipeline_metadata_pipeline_id;
ALTER TABLE pipeline_metadata ADD CONSTRAINT uq_pipeline_metadata_pipeline_id UNIQUE (pipeline_id);

-- Table Notification
CREATE TABLE IF NOT EXISTS notification_log(
    id SERIAL PRIMARY KEY,
    key_notification VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    type VARCHAR(255) NOT NULL,
    is_read BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    iteration_check INTEGER DEFAULT 0, -- For check iteration job, if 3 then will sent into webhook if is_read is false
    is_sent BOOLEAN DEFAULT FALSE,
    is_force_sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Index for notification_log
CREATE INDEX IF NOT EXISTS idx_notification_log_iteration_check ON notification_log(iteration_check);

-- Table queue backfill data 
CREATE TABLE IF NOT EXISTS queue_backfill_data(
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    filter_sql TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'EXECUTING', 'COMPLETED', 'FAILED', 'CANCELLED'
    count_record BIGINT NOT NULL DEFAULT 0,
    total_record BIGINT NOT NULL DEFAULT 0,
    resume_attempts INTEGER NOT NULL DEFAULT 0,
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure error_message column exists (for existing deployments)
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS error_message TEXT NULL;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS total_record BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS resume_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS is_error BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS last_pk_value TEXT NULL;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS pk_column TEXT NULL;

-- Drop Index if exists
DROP INDEX IF EXISTS idx_queue_backfill_data_pipeline_id;
DROP INDEX IF EXISTS idx_queue_backfill_data_source_id;
DROP INDEX IF EXISTS idx_queue_backfill_data_created_at;
DROP INDEX IF EXISTS idx_queue_backfill_data_updated_at;

-- Create Index for queue_backfill_data
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_pipeline_id ON queue_backfill_data(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_source_id ON queue_backfill_data(source_id);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_created_at ON queue_backfill_data(created_at);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_updated_at ON queue_backfill_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_status ON queue_backfill_data(status);

-- Performance indexes for dashboard queries (5 second refresh optimization)

-- System metrics: filtered by recorded_at for date range queries
CREATE INDEX IF NOT EXISTS idx_system_metrics_recorded_at ON system_metrics(recorded_at DESC);

-- Pipelines destination: critical for error tracking and joins
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_pipeline_id ON pipelines_destination(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_is_error ON pipelines_destination(is_error) WHERE is_error = TRUE;
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_last_error_at ON pipelines_destination(last_error_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_destination_id ON pipelines_destination(destination_id);

-- Pipeline destination table sync: for detailed sync monitoring
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_pipeline_dest_id ON pipelines_destination_table_sync(pipeline_destination_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_is_error ON pipelines_destination_table_sync(is_error) WHERE is_error = TRUE;
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_table_name ON pipelines_destination_table_sync(table_name);

-- Pipeline metadata: frequent status checks and activity feed
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_updated_at ON pipeline_metadata(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_last_start_at ON pipeline_metadata(last_start_at DESC);

-- Data flow monitoring: composite index for common date + table queries
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_created_table ON data_flow_record_monitoring(created_at DESC, table_name);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_source_id ON data_flow_record_monitoring(source_id);

-- Notification log: dashboard notification queries
CREATE INDEX IF NOT EXISTS idx_notification_log_is_read ON notification_log(is_read) WHERE is_read = FALSE;
CREATE INDEX IF NOT EXISTS idx_notification_log_is_deleted ON notification_log(is_deleted) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_notification_log_created_at ON notification_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_log_is_sent ON notification_log(is_sent) WHERE is_sent = FALSE;

-- WAL monitor: composite index for status filtering
CREATE INDEX IF NOT EXISTS idx_wal_monitor_updated_at ON wal_monitor(updated_at DESC);

-- Sources: for health monitoring and filtering
CREATE INDEX IF NOT EXISTS idx_sources_is_publication_enabled ON sources(is_publication_enabled);
CREATE INDEX IF NOT EXISTS idx_sources_is_replication_enabled ON sources(is_replication_enabled);

-- Destinations: type-based filtering
CREATE INDEX IF NOT EXISTS idx_destinations_type ON destinations(type);

-- Presets: faster source-based lookups
CREATE INDEX IF NOT EXISTS idx_presets_source_id ON presets(source_id);

-- Job metrics: unique constraint already provides index, add updated_at
CREATE INDEX IF NOT EXISTS idx_job_metrics_monitoring_updated_at ON job_metrics_monitoring(updated_at DESC);

-- Create Table Tagging for smart tag feature
CREATE TABLE IF NOT EXISTS tbltag_list (
    id SERIAL PRIMARY KEY,
    tag VARCHAR(150) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Unique for tag
ALTER TABLE tbltag_list DROP CONSTRAINT IF EXISTS uq_tbltag_list_tag;
ALTER TABLE tbltag_list ADD CONSTRAINT uq_tbltag_list_tag UNIQUE (tag);

-- Create Table for save tagging and pipelines_destination_table_sync
CREATE TABLE IF NOT EXISTS pipelines_destination_table_sync_tag (
    id SERIAL PRIMARY KEY,
    pipelines_destination_table_sync_id INTEGER NOT NULL REFERENCES pipelines_destination_table_sync(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tbltag_list(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for pipelines_destination_table_sync_tag
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_tag_sync_id ON pipelines_destination_table_sync_tag(pipelines_destination_table_sync_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_tag_tag_id ON pipelines_destination_table_sync_tag(tag_id);

-- Add unique constraint to prevent duplicate tags on same table sync (also improves query performance)
ALTER TABLE pipelines_destination_table_sync_tag DROP CONSTRAINT IF EXISTS uq_pipelines_destination_table_sync_tag;
ALTER TABLE pipelines_destination_table_sync_tag ADD CONSTRAINT uq_pipelines_destination_table_sync_tag UNIQUE (pipelines_destination_table_sync_id, tag_id);

-- Composite index for tag usage queries (joining tag → sync → pipeline hierarchy)
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_tag_composite ON pipelines_destination_table_sync_tag(tag_id, pipelines_destination_table_sync_id);

-- Index on created_at for temporal queries and sorting
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_tag_created_at ON pipelines_destination_table_sync_tag(created_at DESC);

-- Performance Optimization for Source Details Page
-- This migration adds composite indexes to optimize the JOIN queries

-- Composite index for history_schema_evolution table
-- Optimizes get_tables_with_version_count query that does:
-- LEFT JOIN history_schema_evolution ON table_metadata_list.id = history_schema_evolution.table_metadata_list_id
-- GROUP BY table_metadata_list.id
-- SELECT MAX(version_schema)
CREATE INDEX IF NOT EXISTS idx_history_schema_evolution_table_version_composite 
ON history_schema_evolution(table_metadata_list_id, version_schema DESC);

-- Additional optimization: covering index for the query
-- This allows index-only scans without touching the table
-- Note: Cannot include schema_table (JSONB) as it can exceed btree index size limit (2704 bytes)
DROP INDEX IF EXISTS idx_table_metadata_list_source_id_covering;
CREATE INDEX idx_table_metadata_list_source_id_covering 
ON table_metadata_list(source_id, id) INCLUDE (table_name);

-- Optimize pipelines_destination queries for source details
-- This composite index helps with the join: pipeline → pipeline_destination → destination
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_composite 
ON pipelines_destination(pipeline_id, destination_id, is_error);

-- Add comment explaining the optimization
COMMENT ON INDEX idx_history_schema_evolution_table_version_composite IS 
'Optimizes source details page query that fetches table version counts with LEFT JOIN and MAX(version_schema)';

COMMENT ON INDEX idx_table_metadata_list_source_id_covering IS 
'Covering index for source details table metadata fetch - includes table_name but not schema_table (JSONB too large for btree)';

COMMENT ON INDEX idx_pipelines_destination_composite IS 
'Composite index for pipeline-destination joins in source details page';


-- Migration 008: Add worker_health_status table
-- This table stores periodic worker health check results

CREATE TABLE IF NOT EXISTS worker_health_status (
    id SERIAL PRIMARY KEY,
    healthy BOOLEAN NOT NULL DEFAULT FALSE,
    active_workers INTEGER NOT NULL DEFAULT 0,
    active_tasks INTEGER NOT NULL DEFAULT 0,
    reserved_tasks INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    extra_data JSONB,
    last_check_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on last_check_at for quick lookups of latest status
CREATE INDEX IF NOT EXISTS idx_worker_health_status_last_check_at ON worker_health_status(last_check_at DESC);

-- Add comment
COMMENT ON TABLE worker_health_status IS 'Stores periodic Celery worker health check results updated every 10 seconds by background task';

-- ============================================================
-- Migration 009: Flow Task — DuckDB Visual ETL Transform Engine
-- ============================================================

-- Table: flow_tasks — master record for each visual ETL flow
CREATE TABLE IF NOT EXISTS flow_tasks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'IDLE',       -- IDLE, RUNNING, SUCCESS, FAILED
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL', -- MANUAL, SCHEDULED
    last_run_at TIMESTAMPTZ NULL,
    last_run_status VARCHAR(20) NULL,                  -- SUCCESS, FAILED, NULL if never run
    last_run_record_count BIGINT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE flow_tasks ADD COLUMN IF NOT EXISTS description TEXT NULL;
ALTER TABLE flow_tasks ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMPTZ NULL;
ALTER TABLE flow_tasks ADD COLUMN IF NOT EXISTS last_run_status VARCHAR(20) NULL;
ALTER TABLE flow_tasks ADD COLUMN IF NOT EXISTS last_run_record_count BIGINT NULL;

CREATE INDEX IF NOT EXISTS idx_flow_tasks_status ON flow_tasks(status);
CREATE INDEX IF NOT EXISTS idx_flow_tasks_last_run_at ON flow_tasks(last_run_at DESC);
COMMENT ON TABLE flow_tasks IS 'Visual ETL flow task definitions — each row is one user-built transform graph';

-- Table: flow_task_graph — persisted node + edge graph (one per flow task, upserted on save)
CREATE TABLE IF NOT EXISTS flow_task_graph (
    id SERIAL PRIMARY KEY,
    flow_task_id INTEGER NOT NULL REFERENCES flow_tasks(id) ON DELETE CASCADE,
    nodes_json JSONB NOT NULL DEFAULT '[]',  -- ReactFlow nodes with position + data
    edges_json JSONB NOT NULL DEFAULT '[]',  -- ReactFlow edges
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_flow_task_graph_flow_task_id UNIQUE (flow_task_id)
);

CREATE INDEX IF NOT EXISTS idx_flow_task_graph_flow_task_id ON flow_task_graph(flow_task_id);
COMMENT ON TABLE flow_task_graph IS 'Persisted ReactFlow node/edge graph for each flow task including node coordinates';
COMMENT ON COLUMN flow_task_graph.nodes_json IS 'JSON array of ReactFlow nodes: [{id, type, position:{x,y}, data:{...node config}}]';
COMMENT ON COLUMN flow_task_graph.edges_json IS 'JSON array of ReactFlow edges: [{id, source, target, sourceHandle, targetHandle}]';

-- Table: flow_task_run_history — one row per execution run
CREATE TABLE IF NOT EXISTS flow_task_run_history (
    id SERIAL PRIMARY KEY,
    flow_task_id INTEGER NOT NULL REFERENCES flow_tasks(id) ON DELETE CASCADE,
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',   -- MANUAL, SCHEDULED
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',        -- RUNNING, SUCCESS, FAILED, CANCELLED
    celery_task_id VARCHAR(255) NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    error_message TEXT NULL,
    total_input_records BIGINT NULL DEFAULT 0,
    total_output_records BIGINT NULL DEFAULT 0,
    run_metadata JSONB NULL,   -- arbitrary per-run context (graph snapshot, etc.)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE flow_task_run_history ADD COLUMN IF NOT EXISTS celery_task_id VARCHAR(255) NULL;
ALTER TABLE flow_task_run_history ADD COLUMN IF NOT EXISTS run_metadata JSONB NULL;
ALTER TABLE flow_task_run_history ADD COLUMN IF NOT EXISTS total_input_records BIGINT NULL DEFAULT 0;
ALTER TABLE flow_task_run_history ADD COLUMN IF NOT EXISTS total_output_records BIGINT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_flow_task_run_history_flow_task_id ON flow_task_run_history(flow_task_id);
CREATE INDEX IF NOT EXISTS idx_flow_task_run_history_status ON flow_task_run_history(status);
CREATE INDEX IF NOT EXISTS idx_flow_task_run_history_started_at ON flow_task_run_history(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_flow_task_run_history_celery_task_id ON flow_task_run_history(celery_task_id);
COMMENT ON TABLE flow_task_run_history IS 'Execution history for flow tasks — one row per triggered run';

-- Table: flow_task_run_node_log — per-node execution stats within a run
CREATE TABLE IF NOT EXISTS flow_task_run_node_log (
    id SERIAL PRIMARY KEY,
    run_history_id INTEGER NOT NULL REFERENCES flow_task_run_history(id) ON DELETE CASCADE,
    flow_task_id INTEGER NOT NULL REFERENCES flow_tasks(id) ON DELETE CASCADE,
    node_id VARCHAR(255) NOT NULL,     -- ReactFlow node id
    node_type VARCHAR(50) NOT NULL,    -- input, clean, aggregate, join, union, pivot, new_rows, output
    node_label VARCHAR(255) NULL,
    row_count_in BIGINT NULL DEFAULT 0,
    row_count_out BIGINT NULL DEFAULT 0,
    duration_ms INTEGER NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, SUCCESS, FAILED, SKIPPED
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flow_task_run_node_log_run_history_id ON flow_task_run_node_log(run_history_id);
CREATE INDEX IF NOT EXISTS idx_flow_task_run_node_log_flow_task_id ON flow_task_run_node_log(flow_task_id);
CREATE INDEX IF NOT EXISTS idx_flow_task_run_node_log_node_type ON flow_task_run_node_log(node_type);
COMMENT ON TABLE flow_task_run_node_log IS 'Per-node execution stats for each flow run — tracks row counts and timing per node';


-- Migration: Add table list tracking to destinations
-- Adds list_tables (JSONB), total_tables (INT), last_table_check_at (TIMESTAMPTZ) to destinations

ALTER TABLE destinations
    ADD COLUMN IF NOT EXISTS list_tables JSONB NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS total_tables INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_table_check_at TIMESTAMPTZ NULL;



-- ============================================================
-- Migration 010: Linked Task — Flow Task Orchestration DAG
-- Allows chaining multiple flow tasks in sequential/parallel
-- patterns with configurable dependency conditions.
-- ============================================================

CREATE TABLE IF NOT EXISTS linked_tasks (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'IDLE',   -- IDLE, RUNNING, SUCCESS, FAILED
    last_run_at TIMESTAMPTZ NULL,
    last_run_status VARCHAR(20) NULL,             -- SUCCESS, FAILED, NULL
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linked_tasks_status ON linked_tasks(status);
CREATE INDEX IF NOT EXISTS idx_linked_tasks_last_run_at ON linked_tasks(last_run_at DESC);
COMMENT ON TABLE linked_tasks IS 'Orchestration DAGs that chain multiple flow_tasks';

-- Steps (nodes) — one row per flow_task placed on the canvas
CREATE TABLE IF NOT EXISTS linked_task_steps (
    id SERIAL PRIMARY KEY,
    linked_task_id INTEGER NOT NULL REFERENCES linked_tasks(id) ON DELETE CASCADE,
    flow_task_id   INTEGER NOT NULL REFERENCES flow_tasks(id)   ON DELETE CASCADE,
    pos_x FLOAT NOT NULL DEFAULT 0,
    pos_y FLOAT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linked_task_steps_linked_task_id ON linked_task_steps(linked_task_id);
COMMENT ON TABLE linked_task_steps IS 'Canvas nodes in a linked_task DAG — each references one flow_task';

-- Edges — dependency connections between steps
CREATE TABLE IF NOT EXISTS linked_task_edges (
    id SERIAL PRIMARY KEY,
    linked_task_id INTEGER NOT NULL REFERENCES linked_tasks(id)        ON DELETE CASCADE,
    source_step_id INTEGER NOT NULL REFERENCES linked_task_steps(id)   ON DELETE CASCADE,
    target_step_id INTEGER NOT NULL REFERENCES linked_task_steps(id)   ON DELETE CASCADE,
    condition VARCHAR(20) NOT NULL DEFAULT 'ON_SUCCESS', -- ON_SUCCESS | ALWAYS
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_linked_task_edge UNIQUE (source_step_id, target_step_id)
);

CREATE INDEX IF NOT EXISTS idx_linked_task_edges_linked_task_id ON linked_task_edges(linked_task_id);
COMMENT ON TABLE linked_task_edges IS 'DAG edges: ON_SUCCESS = run target only if source succeeded; ALWAYS = run regardless';

-- Run history — one row per triggered execution
CREATE TABLE IF NOT EXISTS linked_task_run_history (
    id SERIAL PRIMARY KEY,
    linked_task_id INTEGER NOT NULL REFERENCES linked_tasks(id) ON DELETE CASCADE,
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- RUNNING, SUCCESS, FAILED, CANCELLED
    celery_task_id VARCHAR(255) NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linked_task_run_history_linked_task_id ON linked_task_run_history(linked_task_id);
CREATE INDEX IF NOT EXISTS idx_linked_task_run_history_status ON linked_task_run_history(status);
CREATE INDEX IF NOT EXISTS idx_linked_task_run_history_started_at ON linked_task_run_history(started_at DESC);
COMMENT ON TABLE linked_task_run_history IS 'Execution history for each linked_task DAG run';

-- Step log — per-step status within a run
CREATE TABLE IF NOT EXISTS linked_task_run_step_log (
    id SERIAL PRIMARY KEY,
    run_history_id           INTEGER NOT NULL REFERENCES linked_task_run_history(id) ON DELETE CASCADE,
    linked_task_id           INTEGER NOT NULL REFERENCES linked_tasks(id)            ON DELETE CASCADE,
    step_id                  INTEGER NOT NULL REFERENCES linked_task_steps(id)       ON DELETE CASCADE,
    flow_task_id             INTEGER NOT NULL REFERENCES flow_tasks(id)              ON DELETE CASCADE,
    flow_task_run_history_id INTEGER NULL     REFERENCES flow_task_run_history(id)   ON DELETE SET NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING, RUNNING, SUCCESS, FAILED, SKIPPED
    celery_task_id VARCHAR(255) NULL,
    started_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_linked_task_run_step_log_run_history_id ON linked_task_run_step_log(run_history_id);
CREATE INDEX IF NOT EXISTS idx_linked_task_run_step_log_step_id ON linked_task_run_step_log(step_id);
COMMENT ON TABLE linked_task_run_step_log IS 'Per-step execution logs within a linked_task run';


-- ============================================================
-- Migration 011: Schedules — Cron-based job scheduling
-- Allows users to schedule flow_tasks and linked_tasks to run
-- automatically on a cron expression. APScheduler registers
-- ACTIVE schedules at startup and syncs on CRUD operations.
-- ============================================================

-- Master schedule definition
CREATE TABLE IF NOT EXISTS schedules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT NULL,
    task_type VARCHAR(20) NOT NULL,          -- FLOW_TASK | LINKED_TASK
    task_id INTEGER NOT NULL,                -- references flow_tasks.id or linked_tasks.id
    cron_expression VARCHAR(100) NOT NULL,   -- standard 5-part crontab: "*/5 * * * *"
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | PAUSED
    last_run_at TIMESTAMPTZ NULL,
    next_run_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Unique name constraint (idempotent for re-runs)
ALTER TABLE schedules DROP CONSTRAINT IF EXISTS uq_schedules_name;
ALTER TABLE schedules ADD CONSTRAINT uq_schedules_name UNIQUE (name);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_schedules_status ON schedules(status);
CREATE INDEX IF NOT EXISTS idx_schedules_task_type ON schedules(task_type);
CREATE INDEX IF NOT EXISTS idx_schedules_task_type_task_id ON schedules(task_type, task_id);
CREATE INDEX IF NOT EXISTS idx_schedules_last_run_at ON schedules(last_run_at DESC);

COMMENT ON TABLE schedules IS 'Cron-based job schedules — each row triggers a flow_task or linked_task on a cron expression';
COMMENT ON COLUMN schedules.cron_expression IS 'Standard 5-part crontab string, e.g. "*/5 * * * *" (minute hour day month weekday)';
COMMENT ON COLUMN schedules.task_type IS 'FLOW_TASK = references flow_tasks.id; LINKED_TASK = references linked_tasks.id';

-- Execution run history (append-only, cascade-deleted with schedule)
CREATE TABLE IF NOT EXISTS schedule_run_history (
    id SERIAL PRIMARY KEY,
    schedule_id INTEGER NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    task_type VARCHAR(20) NOT NULL,
    task_id INTEGER NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    duration_ms INTEGER NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING | SUCCESS | FAILED
    message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for run history
CREATE INDEX IF NOT EXISTS idx_schedule_run_history_schedule_id ON schedule_run_history(schedule_id);
CREATE INDEX IF NOT EXISTS idx_schedule_run_history_triggered_at ON schedule_run_history(triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedule_run_history_status ON schedule_run_history(status);
CREATE INDEX IF NOT EXISTS idx_schedule_run_history_schedule_triggered ON schedule_run_history(schedule_id, triggered_at DESC);

COMMENT ON TABLE schedule_run_history IS 'Execution history for scheduled jobs — one row per cron-triggered run, cascade-deleted with parent schedule';


-- ============================================================
-- Migration 009: New Features Bundle
-- B2: Schema Compatibility Validation
-- C2: Batched DLQ Recovery (no schema changes needed)
-- C3: Connection Pool Optimization (no schema changes needed)
-- D2: Data Catalog & Data Dictionary
-- D3: Alerting Rules Engine
-- D4: Flow Task Versioning & Rollback
-- D5: Real-Time Pipeline Metrics Stream (no schema changes needed)
-- D6: SQL Transform Node (no schema changes needed - uses existing node types)
-- D7: Data Profiling on Preview (no schema changes needed)
-- D8: Incremental Flow Task Execution
-- E2: Integration Test Suite (no schema changes needed)
-- ============================================================


-- ============================================================
-- D4: Flow Task Versioning & Rollback
-- ============================================================

CREATE TABLE IF NOT EXISTS flow_task_graph_version (
    id SERIAL PRIMARY KEY,
    flow_task_id INTEGER NOT NULL REFERENCES flow_tasks(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    nodes_json JSONB NOT NULL DEFAULT '[]',
    edges_json JSONB NOT NULL DEFAULT '[]',
    change_summary TEXT NULL,           -- auto-generated or user-provided description
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flow_task_graph_version_flow_task_id ON flow_task_graph_version(flow_task_id);
CREATE INDEX IF NOT EXISTS idx_flow_task_graph_version_version ON flow_task_graph_version(flow_task_id, version DESC);
ALTER TABLE flow_task_graph_version DROP CONSTRAINT IF EXISTS uq_flow_task_graph_version;
ALTER TABLE flow_task_graph_version ADD CONSTRAINT uq_flow_task_graph_version
    UNIQUE (flow_task_id, version);

COMMENT ON TABLE flow_task_graph_version IS 'Versioned snapshots of flow task graphs for rollback support';


-- ============================================================
-- D8: Incremental Flow Task Execution
-- ============================================================

-- Track watermark state per input node per flow task
CREATE TABLE IF NOT EXISTS flow_task_watermarks (
    id SERIAL PRIMARY KEY,
    flow_task_id INTEGER NOT NULL REFERENCES flow_tasks(id) ON DELETE CASCADE,
    node_id VARCHAR(255) NOT NULL,
    watermark_column VARCHAR(255) NOT NULL,
    last_watermark_value TEXT NULL,       -- serialized as string for any type
    watermark_type VARCHAR(50) DEFAULT 'TIMESTAMP', -- TIMESTAMP, INTEGER, UUID
    last_run_at TIMESTAMPTZ NULL,
    record_count BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE flow_task_watermarks DROP CONSTRAINT IF EXISTS uq_flow_task_watermark;
ALTER TABLE flow_task_watermarks ADD CONSTRAINT uq_flow_task_watermark
    UNIQUE (flow_task_id, node_id);

CREATE INDEX IF NOT EXISTS idx_flow_task_watermarks_flow_task_id ON flow_task_watermarks(flow_task_id);

COMMENT ON TABLE flow_task_watermarks IS 'Watermark tracking for incremental flow task execution';


-- ============================================================
-- B2: Schema Compatibility Validation — setting for auto-check
-- ============================================================

INSERT INTO rosetta_setting_configuration(config_key, config_value)
VALUES('SCHEMA_COMPATIBILITY_CHECK_ENABLED', 'TRUE')
ON CONFLICT(config_key) DO NOTHING;


-- ============================================================
-- Performance Optimization: Missing Index Audit
-- Indexes derived from actual ORM/SQL query patterns across
-- backend repositories, compute engine, and background jobs.
-- ============================================================

-- ── High Priority: Background jobs polling every 5-60 seconds ──

-- pipelines.ready_refresh — polled every 10s by pipeline_refresh_check scheduler
CREATE INDEX IF NOT EXISTS idx_pipelines_ready_refresh
    ON pipelines(ready_refresh) WHERE ready_refresh = TRUE;

-- queue_backfill_data(status, created_at) — polled every 5s by compute BackfillManager
-- Covers: WHERE status = 'PENDING' ORDER BY created_at ASC
-- Covers: WHERE status = 'EXECUTING' ORDER BY created_at ASC
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_status_created
    ON queue_backfill_data(status, created_at ASC);

-- notification_log: composite partial for pending notification sender (runs every 30s)
-- Covers: WHERE iteration_check >= N AND is_sent = FALSE AND is_deleted = FALSE AND is_read = FALSE
CREATE INDEX IF NOT EXISTS idx_notification_log_pending_send
    ON notification_log(iteration_check)
    WHERE is_sent = FALSE AND is_deleted = FALSE AND is_read = FALSE;

-- notification_log.key_notification — used by upsert_notification_by_key (runs every 30s)
CREATE INDEX IF NOT EXISTS idx_notification_log_key_notification
    ON notification_log(key_notification);

-- notification_log.is_force_sent — used by forced notification delivery queries
CREATE INDEX IF NOT EXISTS idx_notification_log_is_force_sent
    ON notification_log(is_force_sent) WHERE is_force_sent = TRUE;

-- wal_metrics(source_id, recorded_at) — polled every 60s for "latest WAL by source"
-- Replaces two individual indexes with one compound covering index
CREATE INDEX IF NOT EXISTS idx_wal_metrics_source_recorded
    ON wal_metrics(source_id, recorded_at DESC);

-- ── Medium Priority: User-facing paginated listings ──

-- flow_tasks.updated_at — paginated list sorted by updated_at DESC
CREATE INDEX IF NOT EXISTS idx_flow_tasks_updated_at
    ON flow_tasks(updated_at DESC);

-- schedules.created_at — paginated schedule listing
CREATE INDEX IF NOT EXISTS idx_schedules_created_at
    ON schedules(created_at DESC);

-- linked_tasks.created_at — paginated linked task listing
CREATE INDEX IF NOT EXISTS idx_linked_tasks_created_at
    ON linked_tasks(created_at DESC);

-- flow_task_run_history(flow_task_id, status) — "get latest RUNNING" query
-- Avoids scanning all rows for a flow_task_id when filtering by status
CREATE INDEX IF NOT EXISTS idx_flow_task_run_history_task_status
    ON flow_task_run_history(flow_task_id, status);

-- pipelines_destination_table_sync(pipeline_destination_id, table_name) — compute per-event lookup
-- Covers: WHERE pipeline_destination_id = ? AND table_name = ?
CREATE INDEX IF NOT EXISTS idx_pipelines_dest_table_sync_pd_table
    ON pipelines_destination_table_sync(pipeline_destination_id, table_name);

-- ── Lower Priority: Occasional lookups ──

-- linked_task_run_step_log.linked_task_id — historical lookup by linked task
CREATE INDEX IF NOT EXISTS idx_linked_task_run_step_log_linked_task_id
    ON linked_task_run_step_log(linked_task_id);

-- data_flow_record_monitoring: compound for per-destination-table time-series queries
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_pd_table
    ON data_flow_record_monitoring(pipeline_destination_id, table_name, created_at);

-- data_flow_record_monitoring: FK lookup for pipeline_destination_table_sync_id
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_sync_id
    ON data_flow_record_monitoring(pipeline_destination_table_sync_id);

-- credit_snowflake_monitoring: compound for destination + date range queries
CREATE INDEX IF NOT EXISTS idx_credit_snowflake_dest_date
    ON credit_snowflake_monitoring(destination_id, usage_date);

-- queue_backfill_data: partial index for stale EXECUTING rows (compute health check)
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_executing
    ON queue_backfill_data(updated_at) WHERE status = 'EXECUTING';

