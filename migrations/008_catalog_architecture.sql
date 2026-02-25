-- Migration 008: Catalog Architecture

-- Create catalog databases table
CREATE TABLE IF NOT EXISTS catalog_databases (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description VARCHAR(1000),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_catalog_databases_name ON catalog_databases(name);
COMMENT ON TABLE catalog_databases IS 'Logical database containers in the catalog';

-- Create catalog tables table
CREATE TABLE IF NOT EXISTS catalog_tables (
    id SERIAL PRIMARY KEY,
    database_id INTEGER NOT NULL REFERENCES catalog_databases(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    stream_name VARCHAR(500) NOT NULL,
    source_chain_id VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    last_health_check_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_catalog_tables_db_name UNIQUE (database_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_catalog_tables_database_id ON catalog_tables(database_id);
COMMENT ON TABLE catalog_tables IS 'Table definitions in the catalog';
