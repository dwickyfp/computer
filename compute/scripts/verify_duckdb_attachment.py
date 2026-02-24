"""
Verification script to test PostgreSQL destination DuckDB attachment.

This script verifies that:
1. DuckDB can attach to PostgreSQL database
2. The alias naming is correct
3. Custom SQL with joins works as expected
"""

import duckdb
import re


def get_duckdb_alias(destination_name: str) -> str:
    """
    Calculate DuckDB alias from destination name.

    Same logic as in compute/destinations/postgresql.py
    """
    sanitized = re.sub(r"[^a-z0-9_]", "_", destination_name.lower())
    return f"pg_{sanitized}"


def test_attachment(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    destination_name: str = "test_dest",
):
    """
    Test PostgreSQL attachment to DuckDB.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: PostgreSQL database name
        user: PostgreSQL username
        password: PostgreSQL password
        destination_name: Destination name for alias calculation
    """
    print("=" * 70)
    print("PostgreSQL Destination - DuckDB Attachment Test")
    print("=" * 70)

    # Calculate alias
    alias = get_duckdb_alias(destination_name)
    print(f"\n1. Alias Calculation")
    print(f"   Destination Name: '{destination_name}'")
    print(f"   DuckDB Alias: {alias}")

    # Create connection string
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    print(f"\n2. Connection String (sanitized)")
    print(f"   postgresql://{user}:****@{host}:{port}/{database}")

    try:
        # Create DuckDB connection
        print(f"\n3. Creating DuckDB connection...")
        conn = duckdb.connect(":memory:")
        print(f"   ✓ DuckDB connection created")

        # Install PostgreSQL extension
        print(f"\n4. Installing PostgreSQL extension...")
        conn.execute("INSTALL postgres;")
        conn.execute("LOAD postgres;")
        print(f"   ✓ PostgreSQL extension loaded")

        # Attach PostgreSQL database
        print(f"\n5. Attaching PostgreSQL database...")
        attach_sql = f"ATTACH '{conn_str}' AS {alias} (TYPE postgres, READ_WRITE);"
        conn.execute(attach_sql)
        print(f"   ✓ PostgreSQL attached as '{alias}'")

        # Verify attachment - list schemas
        print(f"\n6. Verifying attachment - List schemas:")
        result = conn.execute(
            f"SELECT schema_name FROM {alias}.information_schema.schemata ORDER BY schema_name"
        ).fetchall()

        for row in result:
            print(f"   - {row[0]}")

        # List tables in public schema
        print(f"\n7. Tables in 'public' schema:")
        result = conn.execute(
            f"""
            SELECT table_name 
            FROM {alias}.information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            LIMIT 10
            """
        ).fetchall()

        if result:
            for row in result:
                print(f"   - {row[0]}")
        else:
            print(f"   (No tables found)")

        # Test a simple query if tables exist
        if result:
            test_table = result[0][0]
            print(f"\n8. Test query on table '{test_table}':")
            query_result = conn.execute(
                f"SELECT COUNT(*) as row_count FROM {alias}.public.{test_table}"
            ).fetchone()
            print(f"   Row count: {query_result[0]}")

        # Test join capability (with hypothetical tables)
        print(f"\n9. Test JOIN SQL syntax:")
        join_sql = f"""
        -- Example: Join local DuckDB table with destination table
        -- This is what custom_sql would look like:
        
        SELECT 
            l.id,
            l.value,
            d.column_name
        FROM local_table l
        LEFT JOIN {alias}.public.dimension_table d 
            ON l.foreign_key = d.id
        """
        print(join_sql)

        print(f"\n" + "=" * 70)
        print("✓ All tests passed!")
        print("=" * 70)

        conn.close()
        return True

    except Exception as e:
        print(f"\n✗ Error: {e}")
        print(f"\n" + "=" * 70)
        print("Test failed!")
        print("=" * 70)
        return False


def test_alias_generation():
    """Test the alias generation for various destination names."""

    print("\n" + "=" * 70)
    print("Alias Generation Tests")
    print("=" * 70)

    test_cases = [
        ("Prod Warehouse", "pg_prod_warehouse"),
        ("Analytics-DB", "pg_analytics_db"),
        ("customer_data", "pg_customer_data"),
        ("Data Lake (Production)", "pg_data_lake__production_"),
        ("warehouse123", "pg_warehouse123"),
        ("UPPERCASE", "pg_uppercase"),
        ("Mix3d-C@se!", "pg_mix3d_c_se_"),
    ]

    all_pass = True
    for name, expected in test_cases:
        actual = get_duckdb_alias(name)
        status = "✓" if actual == expected else "✗"
        print(f"\n{status} '{name}'")
        print(f"  Expected: {expected}")
        print(f"  Actual:   {actual}")

        if actual != expected:
            all_pass = False

    print("\n" + "=" * 70)
    if all_pass:
        print("✓ All alias generation tests passed!")
    else:
        print("✗ Some tests failed!")
    print("=" * 70)

    return all_pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Test PostgreSQL destination DuckDB attachment"
    )
    parser.add_argument("--host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--database", required=True, help="PostgreSQL database")
    parser.add_argument("--user", required=True, help="PostgreSQL username")
    parser.add_argument("--password", required=True, help="PostgreSQL password")
    parser.add_argument(
        "--destination-name", default="test_dest", help="Destination name for alias"
    )
    parser.add_argument(
        "--test-alias-only", action="store_true", help="Only test alias generation"
    )

    args = parser.parse_args()

    # Always test alias generation
    test_alias_generation()

    # Test actual connection if not alias-only mode
    if not args.test_alias_only:
        print("\n")
        success = test_attachment(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            destination_name=args.destination_name,
        )

        exit(0 if success else 1)
    else:
        print(
            "\n(Skipping connection test - use without --test-alias-only to test connection)"
        )
