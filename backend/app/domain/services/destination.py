"""
Destination service containing business logic.

Implements business rules and orchestrates repository operations for destinations.
"""

import json
from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.repositories.destination import DestinationRepository
from app.domain.schemas.destination import DestinationCreate, DestinationUpdate
from app.core.security import encrypt_value, decrypt_value
from app.infrastructure.kafka import create_admin_client
from app.infrastructure.redis import RedisClient

logger = get_logger(__name__)


class DestinationService:
    """
    Service layer for Destination entity.

    Implements business logic for managing Snowflake destination configurations.
    """

    def __init__(self, db: Session):
        """Initialize destination service."""
        self.db = db
        self.repository = DestinationRepository(db)

    @staticmethod
    def _destination_type(
        destination: Destination | DestinationCreate | DestinationUpdate,
    ) -> str:
        return str(getattr(destination, "type", "SNOWFLAKE") or "SNOWFLAKE").upper()

    def _is_kafka_destination(
        self,
        destination: Destination | DestinationCreate | DestinationUpdate,
    ) -> bool:
        return self._destination_type(destination) == "KAFKA"

    def _build_kafka_admin_client_config(
        self, destination: Destination
    ) -> dict[str, Any]:
        config = dict(destination.config or {})
        client = {
            "bootstrap.servers": config.get("bootstrap_servers", ""),
        }
        if config.get("security_protocol"):
            client["security.protocol"] = config["security_protocol"]
        if config.get("sasl_mechanism"):
            client["sasl.mechanism"] = config["sasl_mechanism"]
        if config.get("sasl_username"):
            client["sasl.username"] = config["sasl_username"]
        if config.get("sasl_password"):
            client["sasl.password"] = decrypt_value(config["sasl_password"])
        if config.get("ssl_ca_location"):
            client["ssl.ca.location"] = config["ssl_ca_location"]
        if config.get("ssl_certificate_location"):
            client["ssl.certificate.location"] = config["ssl_certificate_location"]
        if config.get("ssl_key_location"):
            client["ssl.key.location"] = config["ssl_key_location"]
        return client

    def _discover_kafka_topics(self, destination: Destination) -> list[str]:
        if not self._is_kafka_destination(destination):
            raise ValueError(
                "Kafka topic discovery is only available for KAFKA destinations"
            )

        config = dict(destination.config or {})
        bootstrap_servers = config.get("bootstrap_servers", "")
        admin = create_admin_client(self._build_kafka_admin_client_config(destination))
        try:
            metadata = admin.list_topics(timeout=10)
        except Exception as exc:
            raise ValueError(
                "Failed to fetch Kafka metadata from "
                f"'{bootstrap_servers}'. Ensure the broker hostname is reachable "
                "from the backend service. If the bootstrap server is reachable "
                "but metadata still times out, verify Kafka advertised.listeners "
                "is not pointing to an internal hostname such as 'kafka:9092'. "
                f"Original error: {exc}"
            ) from exc

        prefix = str(config.get("topic_prefix") or "").strip()
        prefix_with_dot = f"{prefix}." if prefix else ""
        topics: list[str] = []
        for topic in metadata.topics.keys():
            if topic.startswith("_"):
                continue
            if prefix_with_dot and topic.startswith(prefix_with_dot):
                topics.append(topic.removeprefix(prefix_with_dot))
            elif prefix and topic == prefix:
                continue

        return sorted(set(topics))

    def _cache_table_list(self, destination_id: int, tables: list[str]) -> None:
        try:
            redis_client = RedisClient.get_instance()
            redis_client.setex(
                f"destination:{destination_id}:tables", 600, json.dumps(tables)
            )
        except Exception as e:
            logger.warning(
                "Failed to cache destination tables in Redis",
                extra={"destination_id": destination_id, "error": str(e)},
            )

    def _sync_kafka_topics(self, destination: Destination) -> list[str]:
        tables = self._discover_kafka_topics(destination)
        now = datetime.now(timezone.utc)

        destination.list_tables = tables
        destination.total_tables = len(tables)
        destination.last_table_check_at = now
        self.db.add(destination)
        self.db.flush()

        self._cache_table_list(destination.id, tables)
        return tables

    def create_destination(self, destination_data: DestinationCreate) -> Destination:
        """
        Create a new destination.

        Args:
            destination_data: Destination creation data

        Returns:
            Created destination
        """
        logger.info("Creating new destination", extra={"name": destination_data.name})

        # Default landing configuration to standard configuration if not provided
        if not destination_data.config.get(
            "landing_database"
        ) and destination_data.config.get("database"):
            destination_data.config["landing_database"] = destination_data.config.get(
                "database"
            )

        if not destination_data.config.get(
            "landing_schema"
        ) and destination_data.config.get("schema"):
            destination_data.config["landing_schema"] = destination_data.config.get(
                "schema"
            )

        # Encrypt sensitive fields before storing
        if (
            "password" in destination_data.config
            and destination_data.config["password"]
        ):
            destination_data.config["password"] = encrypt_value(
                destination_data.config["password"]
            )
        if (
            "sasl_password" in destination_data.config
            and destination_data.config["sasl_password"]
        ):
            destination_data.config["sasl_password"] = encrypt_value(
                destination_data.config["sasl_password"]
            )

        if (
            "private_key_passphrase" in destination_data.config
            and destination_data.config["private_key_passphrase"]
        ):
            destination_data.config["private_key_passphrase"] = encrypt_value(
                destination_data.config["private_key_passphrase"]
            )

        destination = self.repository.create(**destination_data.dict())

        if self._is_kafka_destination(destination):
            try:
                self._sync_kafka_topics(destination)
            except Exception as e:
                logger.warning(
                    "Failed to initialize Kafka destination topic cache",
                    extra={"destination_id": destination.id, "error": str(e)},
                )

        logger.info(
            "Destination created successfully",
            extra={"destination_id": destination.id, "name": destination.name},
        )

        return destination

    def get_destination(self, destination_id: int) -> Destination:
        """
        Get destination by ID.

        Args:
            destination_id: Destination identifier

        Returns:
            Destination entity
        """
        return self.repository.get_by_id(destination_id)

    def get_destination_by_name(self, name: str) -> Destination | None:
        """
        Get destination by name.

        Args:
            name: Destination name

        Returns:
            Destination entity or None
        """
        return self.repository.get_by_name(name)

    def list_destinations(self, skip: int = 0, limit: int = 100) -> List[Destination]:
        """
        List all destinations with pagination.

        Args:
            skip: Number of destinations to skip
            limit: Maximum number of destinations to return

        Returns:
            List of destinations
        """
        return self.repository.get_all(skip=skip, limit=limit)

    def count_destinations(self) -> int:
        """
        Count total number of destinations.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_destination(
        self, destination_id: int, destination_data: DestinationUpdate
    ) -> Destination:
        """
        Update destination.

        Args:
            destination_id: Destination identifier
            destination_data: Destination update data

        Returns:
            Updated destination
        """
        logger.info("Updating destination", extra={"destination_id": destination_id})

        # Get existing destination to preserve config values (especially secrets)
        existing_destination = self.repository.get_by_id(destination_id)

        # Filter out None values for partial updates
        update_data = destination_data.dict(exclude_unset=True)

        # Encrypt sensitive fields if provided in update and merge with existing
        if "config" in update_data and update_data["config"]:
            new_config = update_data["config"]

            # Encrypt new secrets if present
            if "password" in new_config and new_config["password"]:
                new_config["password"] = encrypt_value(new_config["password"])
            if "sasl_password" in new_config and new_config["sasl_password"]:
                new_config["sasl_password"] = encrypt_value(new_config["sasl_password"])
            if (
                "private_key_passphrase" in new_config
                and new_config["private_key_passphrase"]
            ):
                new_config["private_key_passphrase"] = encrypt_value(
                    new_config["private_key_passphrase"]
                )
            # Merge: Use old config as base, update with new config
            # This preserves secrets that were filtered out/masked in the frontend
            final_config = existing_destination.config.copy()
            final_config.update(new_config)

            # Update the config in update_data
            update_data["config"] = final_config

        destination = self.repository.update(destination_id, **update_data)

        if self._is_kafka_destination(destination):
            try:
                self._sync_kafka_topics(destination)
            except Exception as e:
                logger.warning(
                    "Failed to refresh Kafka destination topic cache after update",
                    extra={"destination_id": destination.id, "error": str(e)},
                )

        logger.info(
            "Destination updated successfully", extra={"destination_id": destination.id}
        )

        return destination

    def delete_destination(self, destination_id: int) -> None:
        """
        Delete destination.

        Args:
            destination_id: Destination identifier
        """
        logger.info("Deleting destination", extra={"destination_id": destination_id})

        # Collect tag IDs from all pipeline_destinations using this destination
        from app.domain.models.pipeline import PipelineDestination
        from app.domain.models.tag import PipelineDestinationTableSyncTag, TagList

        tag_ids = set()
        pipeline_destinations = (
            self.db.query(PipelineDestination)
            .filter(PipelineDestination.destination_id == destination_id)
            .all()
        )

        for pipeline_dest in pipeline_destinations:
            for table_sync in pipeline_dest.table_syncs:
                for tag_assoc in table_sync.tag_associations:
                    tag_ids.add(tag_assoc.tag_id)

        # Delete destination (CASCADE will delete pipeline_destinations and table_syncs)
        self.repository.delete(destination_id)

        # Cleanup unused tags after deletion
        if tag_ids:
            logger.info(
                "Checking %s tags for cleanup after destination deletion", len(tag_ids)
            )
            # Single bulk query: find which tag_ids are still referenced
            tag_id_list = list(tag_ids)
            used_tag_ids = set(
                row[0]
                for row in self.db.query(PipelineDestinationTableSyncTag.tag_id)
                .filter(PipelineDestinationTableSyncTag.tag_id.in_(tag_id_list))
                .all()
            )
            unused_tag_ids = [tid for tid in tag_id_list if tid not in used_tag_ids]

            if unused_tag_ids:
                # Bulk-load and delete all unused tags in one pass
                unused_tags = (
                    self.db.query(TagList).filter(TagList.id.in_(unused_tag_ids)).all()
                )
                for tag in unused_tags:
                    logger.info(
                        f"Auto-deleting unused tag: {tag.tag}",
                        extra={"tag_id": tag.id, "tag_name": tag.tag},
                    )
                    self.db.delete(tag)

            self.db.commit()

        logger.info(
            "Destination deleted successfully", extra={"destination_id": destination_id}
        )

    def duplicate_destination(self, destination_id: int) -> Destination:
        """
        Duplicate an existing destination.

        Args:
            destination_id: Destination identifier to duplicate

        Returns:
            New destination (duplicate)
        """
        logger.info("Duplicating destination", extra={"destination_id": destination_id})

        # Get the existing destination
        existing_destination = self.get_destination(destination_id)

        # Generate a unique name for the duplicate using a single SQL query
        from sqlalchemy import func, or_

        base_name = existing_destination.name
        # Find all existing names that start with base_name + "_copy" in one query
        pattern = f"{base_name}_copy%"
        existing_copy_names = {
            row[0]
            for row in self.db.query(Destination.name)
            .filter(Destination.name.like(pattern))
            .all()
        }
        copy_number = 1
        new_name = f"{base_name}_copy"
        while new_name in existing_copy_names:
            copy_number += 1
            new_name = f"{base_name}_copy{copy_number}"

        # Create a copy of the configuration (including encrypted secrets)
        config_copy = existing_destination.config.copy()

        # Create a new DestinationCreate object
        destination_data = DestinationCreate(
            name=new_name, type=existing_destination.type, config=config_copy
        )

        # Note: We don't call self.create_destination because it would re-encrypt
        # already encrypted secrets. Instead, we directly use the repository.
        logger.info("Creating duplicate destination", extra={"name": new_name})

        # Default landing configuration to standard configuration if not provided
        if not destination_data.config.get(
            "landing_database"
        ) and destination_data.config.get("database"):
            destination_data.config["landing_database"] = destination_data.config.get(
                "database"
            )

        if not destination_data.config.get(
            "landing_schema"
        ) and destination_data.config.get("schema"):
            destination_data.config["landing_schema"] = destination_data.config.get(
                "schema"
            )

        # Create the new destination (secrets are already encrypted from original)
        new_destination = self.repository.create(**destination_data.dict())

        if self._is_kafka_destination(new_destination):
            try:
                self._sync_kafka_topics(new_destination)
            except Exception as e:
                logger.warning(
                    "Failed to initialize Kafka destination topic cache for duplicate",
                    extra={"destination_id": new_destination.id, "error": str(e)},
                )

        logger.info(
            "Destination duplicated successfully",
            extra={
                "original_id": destination_id,
                "new_id": new_destination.id,
                "new_name": new_name,
            },
        )

        return new_destination

    # [ ... skip to test_connection ... ]

    def test_connection(self, config: DestinationCreate) -> bool:
        """
        Test Snowflake connection for a destination configuration.

        Args:
            config: Destination configuration to test

        Returns:
            True if connection successful

        Raises:
            Exception: If connection fails, with error details
        """
        if config.type == "POSTGRES":
            import psycopg2

            try:
                conn = psycopg2.connect(
                    host=config.config.get("host"),
                    port=config.config.get("port"),
                    dbname=config.config.get("database"),
                    user=config.config.get("user"),
                    password=config.config.get("password"),
                    connect_timeout=5,
                )
                conn.close()
                return True
            except Exception as e:
                logger.error(
                    "Postgres connection test failed",
                    extra={"error": str(e)},
                )
                raise Exception(f"Connection failed: {str(e)}")

        if config.type == "KAFKA":
            try:
                client_config = {
                    "bootstrap.servers": config.config.get("bootstrap_servers"),
                }
                if config.config.get("security_protocol"):
                    client_config["security.protocol"] = config.config[
                        "security_protocol"
                    ]
                if config.config.get("sasl_mechanism"):
                    client_config["sasl.mechanism"] = config.config["sasl_mechanism"]
                if config.config.get("sasl_username"):
                    client_config["sasl.username"] = config.config["sasl_username"]
                if config.config.get("sasl_password"):
                    client_config["sasl.password"] = config.config["sasl_password"]
                admin = create_admin_client(client_config)
                admin.list_topics(timeout=10)
                return True
            except Exception as e:
                logger.error("Kafka connection test failed", extra={"error": str(e)})
                raise Exception(f"Connection failed: {str(e)}")

        # Default to SNOWFLAKE
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        logger.info(
            "Testing connection for destination",
            extra={
                "account": config.config.get("account"),
                "user": config.config.get("user"),
            },
        )

        try:
            if not config.config.get("private_key"):
                # if password authentication
                if config.config.get("password"):
                    # Check connection with password
                    pass
                else:
                    raise ValueError(
                        "Private key or Password is required for connection test"
                    )

            conn_params = {
                "user": config.config.get("user"),
                "account": config.config.get("account"),
                "role": config.config.get("role"),
                "warehouse": config.config.get("warehouse"),
                "database": config.config.get("database"),
                "schema": config.config.get("schema"),
                "client_session_keep_alive": False,
                "application": "Rosetta_ETL",
            }

            # Handle Private Key Auth
            if config.config.get("private_key"):
                # Clean private key string
                private_key_str = config.config.get("private_key", "").strip()
                if "\\n" in private_key_str:
                    private_key_str = private_key_str.replace("\\n", "\n")

                # Handle passphrase
                passphrase = None
                if config.config.get("private_key_passphrase"):
                    passphrase = config.config.get("private_key_passphrase").encode()

                try:
                    # Load private key
                    p_key = serialization.load_pem_private_key(
                        private_key_str.encode(),
                        password=passphrase,
                        backend=default_backend(),
                    )
                except ValueError as ve:
                    logger.error("Failed to load private key: %s", ve)
                    if "Bad decrypt" in str(ve):
                        raise ValueError("Invalid Private Key Passphrase.")
                    raise ValueError("Invalid Private Key format.")

                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                conn_params["private_key"] = pkb

            # Handle Password Auth
            elif config.config.get("password"):
                conn_params["password"] = config.config.get("password")

            # Connect to Snowflake
            ctx = snowflake.connector.connect(**conn_params)

            # Test query
            cs = ctx.cursor()
            cs.execute("SELECT 1")
            result = cs.fetchone()

            cs.close()
            ctx.close()

            if result and result[0] == 1:
                return True
            return False

        except snowflake.connector.errors.ProgrammingError as pe:
            logger.error(
                "Snowflake programming error",
                extra={"error": str(pe)},
            )
            # Catch specific JWT errors to give better hints
            if "JWT token is invalid" in str(pe):
                raise Exception(
                    "Authentication Failed: JWT token is invalid. Please check if the Public Key is correctly assigned to the user in Snowflake, and the Username matches."
                )
            raise pe

        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"error": str(e)},
            )
            # Re-raise with clear message if possible
            raise e

    def fetch_schema(
        self,
        destination_id: int,
        table_name: str | None = None,
        only_tables: bool = False,
    ) -> dict[str, list[str]]:
        """
        Fetch schema (tables and columns) from the destination.

        Args:
            destination_id: Destination identifier
            table_name: Optional table name to filter by
            only_tables: If True, returns only table names (values are empty lists)

        Returns:
            Dictionary mapping table names to list of column names (or empty list if only_tables)
        """
        destination = self.get_destination(destination_id)

        # Redis Key - include table_name/only_tables if provided
        cache_key = f"destination:{destination_id}:schema"
        if table_name:
            cache_key += f":table:{table_name}"
        if only_tables:
            cache_key += ":only_tables"

        try:
            # 1. Try Cache
            redis_client = RedisClient.get_instance()
            cached_schema = redis_client.get(cache_key)
            if cached_schema:
                import json

                return json.loads(cached_schema)
        except Exception as e:
            logger.warning("Redis cache error: %s", e)

        schema_data = {}

        try:
            if destination.type == "POSTGRES":
                import psycopg2

                conn = psycopg2.connect(
                    host=destination.config.get("host"),
                    port=destination.config.get("port"),
                    dbname=destination.config.get("database"),
                    user=destination.config.get("user"),
                    password=decrypt_value(destination.config.get("password") or ""),
                    connect_timeout=10,
                )

                with conn.cursor() as cur:
                    if only_tables:
                        # Fetch ONLY table names
                        query = """
                            SELECT table_name
                            FROM information_schema.tables
                            WHERE table_schema = 'public'
                        """
                        params = []
                        if table_name:
                            query += " AND table_name ILIKE %s"
                            params.append(table_name)
                        query += " ORDER BY table_name;"

                        cur.execute(query, tuple(params))
                        rows = cur.fetchall()
                        for (table,) in rows:
                            schema_data[table] = []
                    else:
                        # Fetch tables and columns from information_schema
                        query = """
                            SELECT table_name, column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                        """
                        params = []
                        if table_name:
                            # Use ILIKE for case-insensitive matching
                            query += " AND table_name ILIKE %s"
                            params.append(table_name)

                        query += " ORDER BY table_name, ordinal_position;"

                        cur.execute(query, tuple(params))
                        rows = cur.fetchall()

                        for table, column in rows:
                            if table not in schema_data:
                                schema_data[table] = []
                            schema_data[table].append(column)

                conn.close()

            elif destination.type == "SNOWFLAKE":
                import snowflake.connector
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization

                conn_params = {
                    "user": destination.config.get("user"),
                    "account": destination.config.get("account"),
                    "role": destination.config.get("role"),
                    "warehouse": destination.config.get("warehouse"),
                    "database": destination.config.get("database"),
                    "schema": destination.config.get("schema"),
                    "client_session_keep_alive": False,
                    "application": "Rosetta_ETL",
                }

                # Handle Private Key Auth
                if destination.config.get("private_key"):
                    private_key_str = destination.config.get("private_key", "").strip()
                    if "\\n" in private_key_str:
                        private_key_str = private_key_str.replace("\\n", "\n")

                    passphrase = None
                    if destination.config.get("private_key_passphrase"):
                        passphrase = decrypt_value(
                            destination.config.get("private_key_passphrase")
                        ).encode()

                    p_key = serialization.load_pem_private_key(
                        private_key_str.encode(),
                        password=passphrase,
                        backend=default_backend(),
                    )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption(),
                    )
                    conn_params["private_key"] = pkb
                elif destination.config.get("password"):
                    conn_params["password"] = decrypt_value(
                        destination.config.get("password")
                    )

                ctx = snowflake.connector.connect(**conn_params)
                cs = ctx.cursor()

                params = []

                if only_tables:
                    # Fetch ONLY table names
                    query = """
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
                    """
                    if table_name:
                        query += " AND TABLE_NAME ILIKE %s"
                        params.append(table_name)

                    query += " ORDER BY TABLE_NAME;"

                    cs.execute(query, tuple(params))
                    rows = cs.fetchall()
                    for (table,) in rows:
                        schema_data[table] = []
                else:
                    # Fetch tables and columns
                    query = """
                        SELECT TABLE_NAME, COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
                    """
                    if table_name:
                        # Use ILIKE for case-insensitive matching in Snowflake
                        query += " AND TABLE_NAME ILIKE %s"
                        params.append(table_name)

                    query += " ORDER BY TABLE_NAME, ORDINAL_POSITION;"

                    cs.execute(query, tuple(params))
                    rows = cs.fetchall()

                    for table, column in rows:
                        if table not in schema_data:
                            schema_data[table] = []
                        schema_data[table].append(column)

                cs.close()
                ctx.close()

            # Cache the result
            try:
                import json

                redis_client = RedisClient.get_instance()
                redis_client.setex(
                    cache_key, 300, json.dumps(schema_data)
                )  # 5 minutes TTL
            except Exception as e:
                logger.warning(
                    "Failed to cache schema for destination %s: %s", destination_id, e
                )

            return schema_data

        except Exception as e:
            logger.error(
                "Failed to fetch schema for destination %s: %s", destination.name, e
            )
            raise ValueError(f"Failed to fetch schema: {str(e)}")

    # ------------------------------------------------------------------
    # Table list (worker-based, async)
    # ------------------------------------------------------------------

    def dispatch_table_list_task(self, destination_id: int) -> Optional[str]:
        """
        Submit a Celery task to the worker to refresh the table list for a destination.

        The task will update list_tables, total_tables, and last_table_check_at
        in the destinations row when it completes.

        Args:
            destination_id: Destination identifier.

        Returns:
            Celery task ID if worker is available, or None if worker is disabled.
        """
        destination = self.get_destination(destination_id)

        if self._is_kafka_destination(destination):
            try:
                self._sync_kafka_topics(destination)
                logger.info(
                    "Kafka destination topics refreshed inline",
                    extra={"destination_id": destination_id},
                )
            except Exception as e:
                logger.error(
                    "Failed to refresh Kafka destination topics inline",
                    extra={"destination_id": destination_id, "error": str(e)},
                )
            return None

        from app.core.config import get_settings

        settings = get_settings()

        if not getattr(settings, "worker_enabled", False):
            logger.warning(
                "Worker disabled – skipping destination table list dispatch",
                extra={"destination_id": destination_id},
            )
            return None

        from app.infrastructure.worker_client import WorkerClient

        try:
            task_id = WorkerClient.get_instance().submit_destination_table_list_task(
                destination_id=destination_id
            )
            logger.info(
                "Destination table list task dispatched",
                extra={"destination_id": destination_id, "task_id": task_id},
            )
            return task_id
        except Exception as e:
            logger.error(
                "Failed to dispatch destination table list task for %s: %s",
                destination_id,
                e,
            )
            return None

    def get_table_list(self, destination_id: int) -> dict:
        """
        Return the table list for a destination.

        Checks Redis cache first (populated by worker after refresh),
        falls back to persisted list_tables in config DB.

        Args:
            destination_id: Destination identifier.

        Returns:
            Dict with tables (list[str]), total_tables (int), last_table_check_at (str|None).
        """
        destination = self.get_destination(destination_id)

        if self._is_kafka_destination(destination):
            try:
                tables = self._sync_kafka_topics(destination)
                last_check = (
                    destination.last_table_check_at.isoformat()
                    if destination.last_table_check_at
                    else None
                )
                return {
                    "tables": tables,
                    "total_tables": len(tables),
                    "last_table_check_at": last_check,
                }
            except Exception as e:
                logger.warning(
                    "Failed to refresh Kafka destination topics, falling back to cached data",
                    extra={"destination_id": destination_id, "error": str(e)},
                )

        # 1. Try Redis cache (set by worker after refresh)
        cache_key = f"destination:{destination_id}:tables"
        try:
            from app.infrastructure.redis import RedisClient
            import json as _json

            redis_client = RedisClient.get_instance()
            cached = redis_client.get(cache_key)
            if cached:
                tables = _json.loads(cached)
                # Still need last_check from DB
                last_check = (
                    destination.last_table_check_at.isoformat()
                    if destination.last_table_check_at
                    else None
                )
                return {
                    "tables": tables,
                    "total_tables": len(tables),
                    "last_table_check_at": last_check,
                }
        except Exception as e:
            logger.warning("Redis cache miss for destination tables: %s", e)

        # 2. Fallback to DB
        tables: list = destination.list_tables if destination.list_tables else []
        last_check = (
            destination.last_table_check_at.isoformat()
            if destination.last_table_check_at
            else None
        )
        return {
            "tables": tables,
            "total_tables": len(tables),
            "last_table_check_at": last_check,
        }

    def refresh_table_list_all(self) -> None:
        """
        Dispatch table list tasks for all destinations.

        Called by the APScheduler job every 30 minutes.
        """
        destinations = self.list_destinations(limit=1000)
        for dest in destinations:
            try:
                self.dispatch_table_list_task(dest.id)
            except Exception as e:
                logger.error(
                    "Failed to dispatch table list task for destination %s: %s",
                    dest.id,
                    e,
                )
