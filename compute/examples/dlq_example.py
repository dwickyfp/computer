#!/usr/bin/env python3
"""
DLQ Example and Test Script (Redis Streams)

Demonstrates basic DLQ functionality including:
- Enqueue messages to Redis Streams
- Check queue status (non-destructive XLEN)
- Dequeue with consumer groups
- Acknowledge processed messages
- Simulate recovery flow
"""

import sys
import os

# Add compute to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.dlq_manager import DLQManager, DLQMessage
from destinations.base import CDCRecord


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KEY_PREFIX = "rosetta:dlq:example"


def example_enqueue():
    """Example: Enqueue messages to DLQ."""
    print("\n=== Example 1: Enqueue Messages ===\n")

    dlq = DLQManager(
        redis_url=REDIS_URL,
        key_prefix=KEY_PREFIX,
    )

    # Create sample CDC records
    records = [
        CDCRecord(
            operation="u",
            table_name="users",
            key={"id": 1},
            value={"id": 1, "name": "Alice", "email": "alice@example.com"},
            timestamp=1678901234567,
        ),
        CDCRecord(
            operation="c",
            table_name="users",
            key={"id": 2},
            value={"id": 2, "name": "Bob", "email": "bob@example.com"},
            timestamp=1678901234568,
        ),
    ]

    # Mock table sync config
    table_sync_config = {
        "id": 1,
        "table_name": "users",
        "table_name_target": "USERS",
        "filter_sql": None,
        "custom_sql": None,
    }

    # Create mock table sync object
    class TableSyncMock:
        def __init__(self, config):
            for k, v in config.items():
                setattr(self, k, v)

    table_sync = TableSyncMock(table_sync_config)

    # Enqueue messages
    for i, record in enumerate(records, 1):
        success = dlq.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=record,
            table_sync=table_sync,
            error_message=f"Test error {i}: Connection refused",
        )
        print(f"Enqueued record {i}: {'✓' if success else '✗'}")

    print(f"\nEnqueued {len(records)} messages to DLQ")


def example_check_status():
    """Example: Check queue status without consuming."""
    print("\n=== Example 2: Check Queue Status ===\n")

    dlq = DLQManager(redis_url=REDIS_URL, key_prefix=KEY_PREFIX)
    queues = dlq.list_queues()

    if not queues:
        print("No queues found")
        return

    print(f"Found {len(queues)} queue(s):")
    for source_id, table_name, dest_id in queues:
        size = dlq.get_queue_size(source_id, table_name, dest_id)
        has_msgs = dlq.has_messages(source_id, table_name, dest_id)
        status = f"📦 {size} messages" if has_msgs else "✓ Empty"
        print(f"  - s{source_id}:t{table_name}:d{dest_id}: {status}")


def example_dequeue():
    """Example: Dequeue and inspect messages using consumer group."""
    print("\n=== Example 3: Dequeue Messages ===\n")

    dlq = DLQManager(redis_url=REDIS_URL, key_prefix=KEY_PREFIX)

    # Dequeue from specific queue via consumer group
    messages = dlq.dequeue_batch(
        source_id=1,
        table_name="users",
        destination_id=2,
        max_messages=10,
        consumer_name="example-consumer",
    )

    if not messages:
        print("No messages in queue")
        return

    print(f"Dequeued {len(messages)} message(s):\n")

    msg_ids = []
    for i, (msg_id, msg) in enumerate(messages, 1):
        msg_ids.append(msg_id)
        print(f"Message {i} (ID: {msg_id}):")
        print(f"  Pipeline ID: {msg.pipeline_id}")
        print(f"  Source ID: {msg.source_id}")
        print(f"  Destination ID: {msg.destination_id}")
        print(f"  Table: {msg.table_name} → {msg.table_name_target}")
        print(f"  Operation: {msg.cdc_record.operation}")
        print(f"  Key: {msg.cdc_record.key}")
        print(f"  Value: {msg.cdc_record.value}")
        print(f"  Retry count: {msg.retry_count}")
        print(f"  First failed at: {msg.first_failed_at}")
        print()

    # Acknowledge all messages (simulating successful processing)
    acked = dlq.acknowledge(
        source_id=1,
        table_name="users",
        destination_id=2,
        message_ids=msg_ids,
    )
    print(f"Acknowledged {acked} messages")


def cleanup():
    """Cleanup test DLQ streams."""
    dlq = DLQManager(redis_url=REDIS_URL, key_prefix=KEY_PREFIX)
    queues = dlq.list_queues()
    for source_id, table_name, dest_id in queues:
        dlq.delete_queue(source_id, table_name, dest_id)
    dlq.close_all()
    print(f"\n✓ Cleaned up {len(queues)} test stream(s)")


def main():
    """Run all examples."""
    print("=" * 60)
    print("DLQ Example Script (Redis Streams)")
    print("=" * 60)

    try:
        # Run examples
        example_enqueue()
        example_check_status()
        example_dequeue()

        # Enqueue again for status check
        print("\n--- Re-enqueueing for status check ---")
        example_enqueue()
        example_check_status()

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        cleanup()


if __name__ == "__main__":
    main()
