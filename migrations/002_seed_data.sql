-- Seed Data for Testing
-- Usage: Execute this in your configuration database

-- 1. Insert Source
INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, publication_name, replication_id)
VALUES (
    'local_postgres_source',
    'localhost',
    5433,
    'postgres',
    'postgres',
    'postgres',
    'my_publication',
    1
)
ON CONFLICT (name) DO UPDATE 
SET pg_host = EXCLUDED.pg_host;

-- 2. Insert Destination
INSERT INTO destinations (
    name, 
    snowflake_account, 
    snowflake_user, 
    snowflake_database, 
    snowflake_schema, 
    snowflake_role, 
    snowflake_private_key_path,
    snowflake_private_key_passphrase
)
VALUES (
    'snowflake_dev',
    'FACAALP-QL94327',
    'ETL_USER',
    'DEVELOPMENT',
    'ETL_SCHEMA',
    'ETL_ROLE',
    '/Users/dwickyferiansyahputra/Public/Research/rosetta/user/rsa_key.p8',
    '123456'
)
ON CONFLICT (name) DO UPDATE 
SET snowflake_account = EXCLUDED.snowflake_account;

-- 3. Insert Pipeline
-- Note: 'name' here maps to the Snowflake Target Table Name in the current implementation
INSERT INTO pipelines (name, source_id, destination_id, status)
VALUES (
    'LANDING_TBL_SALES_DUMMY',
    (SELECT id FROM sources WHERE name = 'local_postgres_source'),
    (SELECT id FROM destinations WHERE name = 'snowflake_dev'),
    'START'
)
ON CONFLICT (name) DO UPDATE 
SET status = 'START';
