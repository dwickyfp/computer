"""
Background worker to monitor the health of Catalog Tables' Redis Streams.
"""

import logging
import threading
import time
from datetime import datetime, timezone
import redis

from config.config import get_config
from core.database import DatabaseSession

logger = logging.getLogger(__name__)


class CatalogHealthWorker:
    """
    Periodically checks if the Redis Streams for Catalog Tables exist and are healthy.
    Updates the table's status in the PostgreSQL database.
    """

    def __init__(self, check_interval_seconds: int = 60):
        self.check_interval_seconds = check_interval_seconds
        self.config = get_config()

        # Initialize Redis connection from DLQ redis_url
        try:
            self.redis_client = redis.Redis.from_url(
                self.config.dlq.redis_url,
                decode_responses=True,
                socket_timeout=5.0,
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize Redis client in CatalogHealthWorker: {e}"
            )
            self.redis_client = None

    def run(self, stop_event: threading.Event) -> None:
        """Run the health check loop."""
        logger.info(
            f"Catalog health worker started (interval={self.check_interval_seconds}s)"
        )

        if not self.redis_client:
            logger.error("Redis client not initialized, exiting CatalogHealthWorker")
            return

        while not stop_event.is_set():
            try:
                self._check_health()
            except Exception as e:
                logger.error(
                    f"Error in Catalog health worker iteration: {e}", exc_info=True
                )

            stop_event.wait(self.check_interval_seconds)

    def _scan_for_stream(self, table_name: str) -> str | None:
        """
        Scan Redis for any rosetta:chain:*:{table_name} key.
        Used as a last-resort fallback when explicit key lookups fail — discovers
        the real key regardless of what source_chain_id is stored.
        """
        pattern = f"rosetta:chain:*:{table_name}"
        try:
            cursor = 0
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor, match=pattern, count=100
                )
                for key in keys:
                    logger.debug(f"Scan found key {key!r} for table {table_name!r}")
                    return key  # return first match
                if cursor == 0:
                    break
        except Exception as e:
            logger.debug(f"Redis scan failed for pattern {pattern}: {e}")
        return None

    def _check_health(self) -> None:
        """Perform the actual health check against the DB and Redis."""
        try:
            with DatabaseSession(autocommit=True) as check_session:
                check_session.execute(
                    "SELECT id, stream_name, source_chain_id, table_name "
                    "FROM catalog_tables"
                )
                tables = check_session.fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch catalog tables for health check: {e}")
            return

        if not tables:
            return  # Nothing to check

        now = datetime.now(timezone.utc)
        updates = []
        stream_name_fixes = []  # rows whose stored stream_name needs correcting

        for table in tables:
            stored_stream = table["stream_name"]
            source_chain_id = table["source_chain_id"]
            table_name = table["table_name"]

            # Canonical key: must match ChainIngestManager.get_stream_key()
            # format: rosetta:chain:{source_chain_id}:{table_name}
            # NOTE: source_chain_id may be a name (set via Backend API sync) rather than
            # the numeric dest ID that _chain_id uses — so the explicit key may be wrong.
            # We always try explicit keys first, then fall back to a wildcard scan.
            if source_chain_id:
                canonical_stream = f"rosetta:chain:{source_chain_id}:{table_name}"
            else:
                canonical_stream = (
                    stored_stream  # will be tried below; scan is fallback
                )

            # Try candidate keys in order: canonical first, then stored
            keys_to_try = list(
                dict.fromkeys(k for k in [canonical_stream, stored_stream] if k)
            )  # deduped, canonical first, None-filtered

            logger.debug(
                f"Health check table={table_name!r} source_chain_id={source_chain_id!r} "
                f"stored={stored_stream!r} keys_to_try={keys_to_try}"
            )

            status = "NO_DATA"
            matched_key = None
            all_not_found = True  # tracks whether every explicit key was "no such key"

            for key in keys_to_try:
                try:
                    stream_info = self.redis_client.xinfo_stream(key)
                    length = stream_info.get("length", 0)
                    # PENDING = stream has unconsumed records
                    # NO_DATA = stream exists but is empty (all consumed / trimmed)
                    status = "PENDING" if length > 0 else "NO_DATA"
                    matched_key = key
                    all_not_found = False
                    logger.debug(f"Stream {key!r}: length={length} → {status}")
                    break
                except redis.exceptions.ResponseError as e:
                    if "no such key" in str(e).lower():
                        logger.debug(f"Stream key not found: {key!r}")
                        continue  # stream not created yet — try next key
                    logger.debug(f"Redis error on {key!r}: {e}")
                    status = "NO_DATA"
                    matched_key = key
                    all_not_found = False
                    break
                except Exception as e:
                    logger.debug(f"Error checking stream {key!r}: {e}")
                    status = "NO_DATA"
                    matched_key = key
                    all_not_found = False
                    break

            # Last resort: wildcard scan when every explicit candidate was missing.
            # This handles the case where source_chain_id was stored as a name
            # (via Backend API sync) but the actual Redis key uses a numeric ID.
            if all_not_found or matched_key is None:
                scanned_key = self._scan_for_stream(table_name)
                if scanned_key:
                    try:
                        stream_info = self.redis_client.xinfo_stream(scanned_key)
                        length = stream_info.get("length", 0)
                        status = "PENDING" if length > 0 else "NO_DATA"
                        matched_key = scanned_key
                        logger.info(
                            f"Discovered real stream key via scan: {scanned_key!r} "
                            f"(table={table_name!r}, length={length})"
                        )
                    except Exception as e:
                        logger.debug(f"Scan key {scanned_key!r} check failed: {e}")

            updates.append(
                {"status": status, "last_health_check_at": now, "table_id": table["id"]}
            )

            # Self-heal: if we found data via a different key than stored,
            # queue an update so future checks use the right key directly.
            # Also extract the real source_chain_id from the key so the
            # canonical lookup works on the next cycle without a Redis scan.
            if matched_key and matched_key != stored_stream:
                # Extract chain_id segment: rosetta:chain:{chain_id}:{table_name}
                parts = matched_key.split(":")
                real_chain_id: str | None = None
                if len(parts) >= 4 and parts[0] == "rosetta" and parts[1] == "chain":
                    real_chain_id = parts[2]

                stream_name_fixes.append(
                    {
                        "stream_name": matched_key,
                        "source_chain_id": real_chain_id,
                        "table_id": table["id"],
                    }
                )
                logger.info(
                    f"Will self-heal stream_name for table {table_name!r}: "
                    f"{stored_stream!r} → {matched_key!r} "
                    f"(source_chain_id → {real_chain_id!r})"
                )

        if updates:
            try:
                with DatabaseSession(autocommit=False) as update_session:
                    update_session.executemany(
                        """
                        UPDATE catalog_tables 
                        SET status = %(status)s, last_health_check_at = %(last_health_check_at)s
                        WHERE id = %(table_id)s
                        """,
                        updates,
                    )
            except Exception as e:
                logger.error(f"Failed to update catalog tables health status: {e}")

        if stream_name_fixes:
            try:
                with DatabaseSession(autocommit=False) as fix_session:
                    fix_session.executemany(
                        "UPDATE catalog_tables "
                        "SET stream_name = %(stream_name)s, "
                        "    source_chain_id = COALESCE(%(source_chain_id)s, source_chain_id) "
                        "WHERE id = %(table_id)s",
                        stream_name_fixes,
                    )
                logger.info(
                    f"Self-healed stream_name for {len(stream_name_fixes)} catalog table(s)"
                )
            except Exception as e:
                logger.error(f"Failed to self-heal catalog table stream_names: {e}")
