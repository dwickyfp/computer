"""
Example: Using Custom SQL with Source and Destination Table Joins

This example demonstrates how to configure a pipeline with custom SQL
that joins CDC source data with:
1. Source database lookup/reference tables
2. Destination dimension tables

Scenario:
- Source: Orders Source (PostgreSQL CDC) with reference tables
- Destination: Prod Warehouse (PostgreSQL) with dimension tables
- Goal: Enrich orders with product info from source AND customer data from destination
"""

import httpx
import asyncio

# API configuration
API_BASE_URL = "http://localhost:8000/api/v1"


async def create_enriched_pipeline():
    """
    Create a pipeline that enriches order CDC data with:
    - Product info from source database
    - Customer dimensions from destination database
    """

    async with httpx.AsyncClient() as client:

        # Step 1: Create source (PostgreSQL with CDC)
        source_payload = {
            "name": "Orders Source",  # Will become alias: pg_src_orders_source
            "type": "POSTGRES",
            "config": {
                "host": "localhost",
                "port": 5434,
                "database": "source_db",
                "user": "postgres",
                "password": "postgres",
            },
        }

        response = await client.post(f"{API_BASE_URL}/sources", json=source_payload)
        source = response.json()
        source_id = source["id"]
        print(f"✓ Created source: {source['name']} (ID: {source_id})")
        print(f"  DuckDB alias will be: pg_src_orders_source")

        # Step 2: Create destination (PostgreSQL warehouse)
        dest_payload = {
            "name": "Prod Warehouse",  # Will become alias: pg_prod_warehouse
            "type": "POSTGRES",
            "config": {
                "host": "localhost",
                "port": 5432,
                "database": "warehouse",
                "user": "warehouse_user",
                "password": "warehouse_pass",
                "schema": "public",
            },
        }

        response = await client.post(f"{API_BASE_URL}/destinations", json=dest_payload)
        destination = response.json()
        dest_id = destination["id"]
        print(f"✓ Created destination: {destination['name']} (ID: {dest_id})")
        print(f"  DuckDB alias will be: pg_prod_warehouse")

        # Step 3: Create pipeline
        pipeline_payload = {
            "name": "Orders with Multi-Database Enrichment",
            "source_id": source_id,
            "destination_ids": [dest_id],
            "table_names": ["tbl_orders"],
        }

        response = await client.post(f"{API_BASE_URL}/pipelines", json=pipeline_payload)
        pipeline = response.json()
        pipeline_id = pipeline["id"]
        print(f"✓ Created pipeline: {pipeline['name']} (ID: {pipeline_id})")

        # Step 4: Configure table sync with custom SQL joining BOTH source and destination
        # This enriches orders with product info from source AND customer data from destination
        custom_sql = """
        SELECT 
            o.order_id,
            o.product_id,
            o.customer_id,
            o.order_date,
            o.quantity,
            o.amount,
            o.status,
            -- Product info from SOURCE database lookup table
            p.product_name,
            p.category,
            p.unit_price,
            p.sku,
            -- Customer info from DESTINATION dimension table
            c.customer_name,
            c.customer_email,
            c.customer_segment,
            c.region,
            c.lifetime_value
        FROM tbl_orders o
        LEFT JOIN pg_src_orders_source.public.ref_products p 
            ON o.product_id = p.product_id
        LEFT JOIN pg_prod_warehouse.public.dim_customers c 
            ON o.customer_id = c.customer_id
        WHERE o.amount > 0
        """

        table_sync_payload = {
            "table_name": "tbl_orders",
            "table_name_target": "fact_orders_enriched",
            "filter_sql": None,
            "custom_sql": custom_sql,
        }

        response = await client.post(
            f"{API_BASE_URL}/pipelines/{pipeline_id}/destinations/{dest_id}/table-syncs",
            json=table_sync_payload,
        )
        table_sync = response.json()
        print(f"✓ Configured table sync with multi-database custom SQL")
        print(f"  Source table: {table_sync['table_name']}")
        print(f"  Target table: {table_sync['table_name_target']}")
        print(f"  Joins:")
        print(f"    - Source:      pg_src_orders_source.public.ref_products")
        print(f"    - Destination: pg_prod_warehouse.public.dim_customers")

        # Step 5: Start pipeline
        response = await client.post(f"{API_BASE_URL}/pipelines/{pipeline_id}/start")
        print(f"✓ Pipeline started!")

        return pipeline_id


async def example_source_lookup_only():
    """
    Example: Join CDC data with source database lookup tables only.
    """

    custom_sql = """
    SELECT 
        t.transaction_id,
        t.account_id,
        t.transaction_type,
        t.amount,
        t.timestamp,
        
        -- Join with source account lookup table
        a.account_name,
        a.account_type,
        a.branch_code,
        a.status,
        
        -- Join with source transaction code lookup
        tc.description as transaction_description,
        tc.category,
        tc.requires_approval
        
    FROM tbl_transactions t
    
    LEFT JOIN pg_src_banking_source.public.accounts a
        ON t.account_id = a.account_id
        
    LEFT JOIN pg_src_banking_source.public.transaction_codes tc
        ON t.transaction_type = tc.code
    """

    print("Source lookup only SQL:")
    print(custom_sql)

    return custom_sql


async def example_multi_table_join():
    """
    Example: Join CDC data with multiple dimension tables.
    """

    custom_sql = """
    SELECT 
        s.sale_id,
        s.product_id,
        s.customer_id,
        s.store_id,
        s.sale_date,
        s.quantity,
        s.amount,
        
        -- Join with products dimension
        p.product_name,
        p.category,
        p.brand,
        p.unit_price,
        
        -- Join with customers dimension
        c.customer_name,
        c.customer_segment,
        c.region,
        
        -- Join with stores dimension
        st.store_name,
        st.store_type,
        st.city,
        st.state
        
    FROM tbl_sales s
    
    LEFT JOIN pg_prod_warehouse.dimensions.products p 
        ON s.product_id = p.product_id
        
    LEFT JOIN pg_prod_warehouse.dimensions.customers c 
        ON s.customer_id = c.customer_id
        
    LEFT JOIN pg_prod_warehouse.dimensions.stores st 
        ON s.store_id = st.store_id
        
    WHERE s.amount > 0
    """

    print("Multi-table join SQL:")
    print(custom_sql)

    return custom_sql


async def example_aggregation_with_history():
    """
    Example: Calculate metrics using historical data from destination.
    """

    custom_sql = """
    SELECT 
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.amount,
        t.transaction_type,
        
        -- Calculate running balance from historical data
        COALESCE(
            (SELECT SUM(amount) 
             FROM pg_prod_warehouse.public.transactions hist
             WHERE hist.account_id = t.account_id 
               AND hist.transaction_date < t.transaction_date
            ), 
            0
        ) as previous_balance,
        
        -- Add current transaction
        COALESCE(
            (SELECT SUM(amount) 
             FROM pg_prod_warehouse.public.transactions hist
             WHERE hist.account_id = t.account_id 
               AND hist.transaction_date < t.transaction_date
            ), 
            0
        ) + t.amount as new_balance,
        
        -- Count previous transactions
        (SELECT COUNT(*) 
         FROM pg_prod_warehouse.public.transactions hist
         WHERE hist.account_id = t.account_id 
           AND hist.transaction_date < t.transaction_date
        ) as transaction_count
        
    FROM tbl_transactions t
    """

    print("Aggregation with history SQL:")
    print(custom_sql)

    return custom_sql


async def example_lookup_enrichment():
    """
    Example: Enrich CDC data with lookup/reference tables.
    """

    custom_sql = """
    SELECT 
        e.event_id,
        e.event_type,
        e.status_code,
        e.severity,
        e.timestamp,
        e.message,
        
        -- Add status description from lookup
        s.status_description,
        s.category,
        s.is_critical,
        
        -- Add severity mapping
        sev.severity_level,
        sev.notification_required,
        sev.escalation_level
        
    FROM tbl_events e
    
    LEFT JOIN pg_prod_warehouse.metadata.status_lookup s 
        ON e.status_code = s.code
        
    LEFT JOIN pg_prod_warehouse.metadata.severity_mapping sev 
        ON e.severity = sev.severity_code
    """

    print("Lookup enrichment SQL:")
    print(custom_sql)

    return custom_sql


def get_destination_alias(destination_name: str) -> str:
    """
    Calculate the DuckDB alias for a destination name.

    Rules:
    - Lowercase
    - Replace spaces and special chars with underscores
    - Prefix with 'pg_'
    """
    import re

    sanitized = re.sub(r"[^a-z0-9_]", "_", destination_name.lower())
    return f"pg_{sanitized}"


def get_source_alias(source_name: str) -> str:
    """
    Calculate the DuckDB alias for a source name.

    Rules:
    - Lowercase
    - Replace spaces and special chars with underscores
    - Prefix with 'pg_src_'
    """
    import re

    sanitized = re.sub(r"[^a-z0-9_]", "_", source_name.lower())
    return f"pg_src_{sanitized}"


def print_examples():
    """Print name to alias mapping examples."""

    examples = [
        ("destination", "Prod Warehouse"),
        ("destination", "Analytics-DB"),
        ("destination", "customer_data"),
        ("source", "Orders Source"),
        ("source", "Production-DB"),
        ("source", "Banking System"),
    ]

    print("\n" + "=" * 60)
    print("Name → DuckDB Alias Mapping")
    print("=" * 60)

    for db_type, name in examples:
        if db_type == "destination":
            alias = get_destination_alias(name)
            print(f"  Destination '{name}' → {alias}")
        else:
            alias = get_source_alias(name)
            print(f"  Source '{name}' → {alias}")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("PostgreSQL Custom SQL with Source & Destination Joins")
    print("=" * 60 + "\n")

    # Show alias mapping
    print_examples()

    print("\nExample 1: Multi-database enrichment (source + destination)")
    print("-" * 60)
    asyncio.run(create_enriched_pipeline())

    print("\n\nExample 2: Source lookup only")
    print("-" * 60)
    asyncio.run(example_source_lookup_only())

    print("\n\nExample 3: Multi-table join (destination dimensions)")
    print("-" * 60)
    asyncio.run(example_multi_table_join())

    print("\n\nExample 4: Aggregation with historical data")
    print("-" * 60)
    asyncio.run(example_aggregation_with_history())

    print("\n\nExample 5: Lookup table enrichment")
    print("-" * 60)
    asyncio.run(example_lookup_enrichment())

    print("\n" + "=" * 60)
    print("For more details, see: compute/destinations/POSTGRESQL_CUSTOM_SQL.md")
    print("=" * 60)
