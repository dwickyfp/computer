#!/usr/bin/env python3
"""
Rosetta Compute Engine - Performance Monitoring Script

Monitors key metrics for high-scale deployments (20+ pipelines, 100+ tables).

Usage:
    python scripts/monitor_performance.py
    python scripts/monitor_performance.py --watch  # Continuous monitoring
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import psycopg2
import redis
from psycopg2.extras import RealDictCursor


class Colors:
    """ANSI color codes for terminal output."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def get_config_db_connection():
    """Get connection to Rosetta config database."""
    db_url = os.getenv(
        "CONFIG_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5433/rosetta",
    )
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def get_redis_connection():
    """Get connection to Redis (DLQ storage)."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(redis_url)


def check_pipeline_health() -> Tuple[int, int, List[Dict]]:
    """
    Check pipeline health status.

    Returns:
        (active_count, total_count, unhealthy_pipelines)
    """
    conn = get_config_db_connection()
    try:
        with conn.cursor() as cur:
            # Get total and active pipelines
            cur.execute("SELECT COUNT(*) as total FROM pipelines")
            total = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) as active FROM pipelines WHERE status = 'START'")
            active = cur.fetchone()["active"]

            # Get unhealthy pipeline metadata
            cur.execute(
                """
                SELECT 
                    p.id,
                    p.name,
                    p.status,
                    pm.health_status,
                    pm.last_heartbeat_at,
                    pm.error_message,
                    pg_size_pretty(pm.wal_size) as wal_size
                FROM pipelines p
                LEFT JOIN pipeline_metadata pm ON p.id = pm.pipeline_id
                WHERE p.status = 'START' 
                  AND (pm.health_status IS NULL OR pm.health_status != 'HEALTHY')
                ORDER BY pm.last_heartbeat_at DESC
            """
            )
            unhealthy = cur.fetchall()

            return active, total, unhealthy
    finally:
        conn.close()


def check_database_connections() -> Dict[str, int]:
    """
    Check database connection usage.

    Returns:
        Dict mapping application_name to connection count
    """
    conn = get_config_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    COALESCE(application_name, 'unknown') as app_name,
                    COUNT(*) as conn_count
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                GROUP BY application_name
                ORDER BY conn_count DESC
            """
            )
            results = cur.fetchall()

            return {row["app_name"]: row["conn_count"] for row in results}
    finally:
        conn.close()


def check_source_replication_lag() -> List[Dict]:
    """
    Check replication slot lag on source databases.

    Returns:
        List of dicts with slot info (only for accessible sources)
    """
    config_conn = get_config_db_connection()
    lag_info = []

    try:
        with config_conn.cursor() as cur:
            # Get all active sources
            cur.execute(
                """
                SELECT DISTINCT
                    s.id,
                    s.name,
                    s.pg_host,
                    s.pg_port,
                    s.pg_database,
                    s.pg_username,
                    s.pg_password,
                    s.replication_name as slot_name
                FROM sources s
                JOIN pipelines p ON p.source_id = s.id
                WHERE p.status = 'START'
            """
            )
            sources = cur.fetchall()

        # Check each source
        for source in sources:
            try:
                # Note: In production, passwords should be decrypted
                source_conn = psycopg2.connect(
                    host=source["pg_host"],
                    port=source["pg_port"],
                    dbname=source["pg_database"],
                    user=source["pg_username"],
                    password=source["pg_password"],  # Should decrypt in real usage
                    cursor_factory=RealDictCursor,
                    connect_timeout=5,
                )

                with source_conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 
                            slot_name,
                            active,
                            pg_size_pretty(
                                pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                            ) AS lag,
                            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS lag_bytes
                        FROM pg_replication_slots
                        WHERE slot_name = %s
                    """,
                        (source["slot_name"],),
                    )
                    slot_info = cur.fetchone()

                    if slot_info:
                        lag_info.append(
                            {
                                "source_name": source["name"],
                                "slot_name": slot_info["slot_name"],
                                "active": slot_info["active"],
                                "lag": slot_info["lag"],
                                "lag_bytes": slot_info["lag_bytes"] or 0,
                            }
                        )

                source_conn.close()

            except Exception as e:
                lag_info.append(
                    {
                        "source_name": source["name"],
                        "slot_name": source["slot_name"],
                        "active": None,
                        "lag": "ERROR",
                        "lag_bytes": 0,
                        "error": str(e),
                    }
                )

    finally:
        config_conn.close()

    return lag_info


def check_dlq_depth() -> List[Dict]:
    """
    Check DLQ message depth per pipeline/destination.

    Returns:
        List of dicts with stream info
    """
    r = get_redis_connection()
    key_prefix = os.getenv("DLQ_KEY_PREFIX", "rosetta:dlq")

    # Scan for all DLQ streams
    streams = []
    pattern = f"{key_prefix}:*"

    for key in r.scan_iter(match=pattern, count=100):
        key_str = key.decode("utf-8") if isinstance(key, bytes) else key
        depth = r.xlen(key_str)

        if depth > 0:
            # Parse key: rosetta:dlq:pipeline_X:destination_Y:table_Z
            parts = key_str.split(":")
            info = {
                "key": key_str,
                "depth": depth,
            }

            # Extract pipeline/destination/table if possible
            for part in parts:
                if part.startswith("pipeline_"):
                    info["pipeline_id"] = part.replace("pipeline_", "")
                elif part.startswith("destination_"):
                    info["destination_id"] = part.replace("destination_", "")
                elif part.startswith("table_"):
                    info["table_name"] = part.replace("table_", "")

            streams.append(info)

    # Sort by depth descending
    streams.sort(key=lambda x: x["depth"], reverse=True)

    return streams


def check_system_resources() -> Dict:
    """
    Check system resource usage.

    Returns:
        Dict with memory and CPU info
    """
    import subprocess

    resources = {}

    try:
        # Count pipeline processes
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, check=True
        )
        pipeline_processes = [
            line for line in result.stdout.split("\n") if "Pipeline_" in line
        ]
        resources["pipeline_process_count"] = len(pipeline_processes)

        # Calculate total memory (in MB)
        total_mem = 0
        total_cpu = 0.0
        for line in pipeline_processes:
            parts = line.split()
            if len(parts) >= 11:
                # Column 3 is %CPU, Column 5 is RSS (KB)
                try:
                    cpu = float(parts[2])
                    mem_kb = float(parts[5])
                    total_cpu += cpu
                    total_mem += mem_kb / 1024  # Convert to MB
                except (ValueError, IndexError):
                    pass

        resources["total_memory_mb"] = round(total_mem, 2)
        resources["total_cpu_pct"] = round(total_cpu, 2)

    except Exception as e:
        resources["error"] = str(e)

    return resources


def print_section(title: str):
    """Print section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{title}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 80}{Colors.END}")


def print_status_line(label: str, value: str, status: str = "info"):
    """Print status line with color."""
    color = Colors.GREEN
    if status == "warning":
        color = Colors.YELLOW
    elif status == "error":
        color = Colors.RED

    print(f"{label}: {color}{value}{Colors.END}")


def main(watch: bool = False, interval: int = 10):
    """Main monitoring function."""

    while True:
        # Clear screen on watch mode
        if watch:
            os.system("clear" if os.name == "posix" else "cls")

        print(f"\n{Colors.BOLD}Rosetta Compute Engine - Performance Monitor{Colors.END}")
        print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")

        # 1. Pipeline Health
        print_section("Pipeline Health")
        try:
            active, total, unhealthy = check_pipeline_health()
            status = "info" if len(unhealthy) == 0 else "warning"
            print_status_line(
                "Active Pipelines", f"{active}/{total}", status
            )

            if unhealthy:
                print(f"\n{Colors.RED}Unhealthy Pipelines:{Colors.END}")
                for p in unhealthy[:5]:  # Show first 5
                    print(
                        f"  - Pipeline {p['id']} ({p['name']}): "
                        f"status={p['health_status']}, "
                        f"last_heartbeat={p['last_heartbeat_at']}, "
                        f"wal_size={p['wal_size']}"
                    )
                    if p["error_message"]:
                        print(f"    Error: {p['error_message'][:100]}...")
        except Exception as e:
            print_status_line("Error", str(e), "error")

        # 2. Database Connections
        print_section("Database Connections")
        try:
            connections = check_database_connections()
            total_conn = sum(connections.values())

            # Determine status based on connection count
            status = "info"
            if total_conn > 150:
                status = "error"
            elif total_conn > 100:
                status = "warning"

            print_status_line("Total Connections", str(total_conn), status)
            print("\nBy Application:")
            for app, count in list(connections.items())[:10]:
                print(f"  - {app}: {count}")

        except Exception as e:
            print_status_line("Error", str(e), "error")

        # 3. Replication Lag
        print_section("Replication Lag")
        try:
            lag_info = check_source_replication_lag()

            if not lag_info:
                print("No replication slots found or sources not accessible")
            else:
                for info in lag_info:
                    # Determine status based on lag (5GB threshold)
                    status = "info"
                    if "error" in info:
                        status = "error"
                        print_status_line(
                            f"{info['source_name']} ({info['slot_name']})",
                            f"ERROR: {info['error']}",
                            status,
                        )
                    else:
                        if info["lag_bytes"] > 5 * 1024 * 1024 * 1024:  # 5GB
                            status = "error"
                        elif info["lag_bytes"] > 1 * 1024 * 1024 * 1024:  # 1GB
                            status = "warning"

                        print_status_line(
                            f"{info['source_name']} ({info['slot_name']})",
                            f"{info['lag']} (active: {info['active']})",
                            status,
                        )

        except Exception as e:
            print_status_line("Error", str(e), "error")

        # 4. DLQ Depth
        print_section("Dead Letter Queue (DLQ)")
        try:
            streams = check_dlq_depth()

            if not streams:
                print_status_line("DLQ Status", "Empty (no failed messages)", "info")
            else:
                total_messages = sum(s["depth"] for s in streams)
                status = "warning" if total_messages > 1000 else "info"
                print_status_line(
                    "Total Messages", f"{total_messages:,}", status
                )

                print("\nTop 5 Queues:")
                for stream in streams[:5]:
                    pipeline = stream.get("pipeline_id", "?")
                    dest = stream.get("destination_id", "?")
                    table = stream.get("table_name", "?")
                    status = "error" if stream["depth"] > 10000 else "warning"
                    print_status_line(
                        f"  Pipeline {pipeline} -> Dest {dest} -> {table}",
                        f"{stream['depth']:,} messages",
                        status,
                    )

        except Exception as e:
            print_status_line("Error", str(e), "error")

        # 5. System Resources
        print_section("System Resources")
        try:
            resources = check_system_resources()

            if "error" in resources:
                print_status_line("Error", resources["error"], "error")
            else:
                # Process count
                expected = 20  # Adjust based on your configuration
                status = (
                    "info"
                    if resources["pipeline_process_count"] == expected
                    else "warning"
                )
                print_status_line(
                    "Pipeline Processes",
                    f"{resources['pipeline_process_count']} (expected: {expected})",
                    status,
                )

                # Memory
                mem_status = "info"
                if resources["total_memory_mb"] > 20000:
                    mem_status = "error"
                elif resources["total_memory_mb"] > 16000:
                    mem_status = "warning"

                print_status_line(
                    "Total Memory",
                    f"{resources['total_memory_mb']:,.0f} MB",
                    mem_status,
                )

                # CPU
                cpu_status = "info"
                if resources["total_cpu_pct"] > 600:
                    cpu_status = "error"
                elif resources["total_cpu_pct"] > 400:
                    cpu_status = "warning"

                print_status_line(
                    "Total CPU",
                    f"{resources['total_cpu_pct']:.1f}%",
                    cpu_status,
                )

        except Exception as e:
            print_status_line("Error", str(e), "error")

        print(f"\n{Colors.BLUE}{'=' * 80}{Colors.END}\n")

        if not watch:
            break

        print(f"Refreshing in {interval} seconds... (Ctrl+C to stop)")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor Rosetta Compute Engine performance metrics"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Continuously monitor and refresh display",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Refresh interval in seconds (default: 10)",
    )

    args = parser.parse_args()
    main(watch=args.watch, interval=args.interval)
