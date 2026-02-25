-- Migration 009: Catalog Pipeline Source

-- Add catalog_table_id to pipelines table
ALTER TABLE pipelines 
ADD COLUMN IF NOT EXISTS catalog_table_id INTEGER REFERENCES catalog_tables(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_pipelines_catalog_table_id ON pipelines(catalog_table_id);
COMMENT ON COLUMN pipelines.catalog_table_id IS 'Reference to catalog table (only when source_type=CATALOG_TABLE)';
