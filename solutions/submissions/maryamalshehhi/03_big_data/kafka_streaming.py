"""
StackUp Engineering Academy — Data Engineering Assessment
Pillar 3 — Big Data Processing
Author: maryamalshehhi

Task covered:
  Task 3.2 -> create_topics() / producer / consumer / escalation forwarding / summary

NOT LIVE-VERIFIED IN THIS SESSION: this machine has no Docker (Kafka broker
requires it) and Docker Desktop needs an admin-elevated install this
environment can't perform. This code is written to run correctly against
the docker-compose.yml Kafka service (localhost:9092) once Docker is
available; it has not been run against a live broker.

HOW TO RUN (once `docker compose up -d` shows kafka as Up):
  pip install kafka-python
  # Terminal 1
  python solutions/submissions/maryamalshehhi/03_big_data/kafka_streaming.py --mode producer
  # Terminal 2
  python solutions/submissions/maryamalshehhi/03_big_data/kafka_streaming.py --mode consumer
  # Or, for a quick local test:
  python solutions/submissions/maryamalshehhi/03_big_data/kafka_streaming.py --mode both

Output:
  outputs/kafka/summary.json
"""

import json
import os
import time
import argparse
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
EVENTS_FILE = os.path.join(BASE_DIR, "datasets", "events_stream", "events_2025_01.jsonl")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "kafka")
os.makedirs(OUTPUT_DIR, exist_ok=True)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS = "presight.project.events"
TOPIC_ESCALATIONS = "presight.escalations.critical"
CONSUMER_GROUP = "presight-assessment-consumer"

PRODUCE_DELAY_SECONDS = 0.05  # 50ms between messages, per Task 3.2b
LOG_EVERY = 100


# ==============================================================================
# TASK 3.2a — Setup
# ==============================================================================

def create_topics():
    """
    Create the two required topics. Handles the "already exists" case
    gracefully so re-running this script (e.g. producer then consumer, both
    calling create_topics()) never fails on the second call.
    """
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="presight-admin")
    topics = [
        NewTopic(name=TOPIC_EVENTS, num_partitions=3, replication_factor=1),
        NewTopic(name=TOPIC_ESCALATIONS, num_partitions=1, replication_factor=1),
    ]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        logger.info("Created topics: %s, %s", TOPIC_EVENTS, TOPIC_ESCALATIONS)
    except TopicAlreadyExistsError:
        logger.info("Topics already exist — skipping creation.")
    finally:
        admin.close()


# ==============================================================================
# TASK 3.2b — Producer
# ==============================================================================

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        request_timeout_ms=30000,
    )


def run_producer(producer: KafkaProducer, events_file: str, delay_seconds: float = PRODUCE_DELAY_SECONDS) -> int:
    """
    Streams events_2025_01.jsonl (8,333 events) to TOPIC_EVENTS one message
    at a time, keyed by event_type (so all events of a type land on the same
    partition — useful for a downstream consumer that needs per-type
    ordering), with a produced_at timestamp added so the consumer can later
    measure end-to-end latency.
    """
    logger.info("Starting producer — streaming events from: %s", events_file)
    sent = 0
    with open(events_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            event["produced_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(TOPIC_EVENTS, key=event.get("event_type"), value=event)
            sent += 1
            if sent % LOG_EVERY == 0:
                logger.info("Produced %d messages (last: %s / %s)", sent, event["event_id"], event["event_type"])
            time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    logger.info("Producer done — %d messages sent to %s", sent, TOPIC_EVENTS)
    return sent


# ==============================================================================
# TASK 3.2c / 3.2d — Consumer + escalation forwarding
# ==============================================================================

def build_consumer(topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,  # stop after 10s of no new messages
    )


def run_consumer(consumer: KafkaConsumer) -> dict:
    logger.info("Starting consumer — listening on: %s", TOPIC_EVENTS)

    forwarder = build_producer()
    event_counts = {}
    consumed = 0
    forwarded = 0
    start = time.time()

    try:
        for message in consumer:
            event = message.value
            consumed += 1
            event_type = event.get("event_type", "unknown")
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

            if consumed % LOG_EVERY == 0:
                logger.info("Consumed %d messages (last: %s / %s / project=%s)",
                            consumed, event.get("event_id"), event_type, event.get("project_id"))

            # Task 3.2d: forward Critical escalations to their own topic.
            if event_type == "escalation_raised" and (event.get("payload") or {}).get("severity") == "Critical":
                forwarder.send(TOPIC_ESCALATIONS, key=event.get("project_id"), value=event)
                forwarded += 1
    finally:
        # Ensures the summary is still written and connections are released
        # even if the broker drops mid-stream, rather than leaking both
        # clients and losing every count gathered so far.
        forwarder.flush()
        forwarder.close()
        consumer.close()

    elapsed = time.time() - start
    logger.info("Consumer done — %d messages consumed, %d critical escalations forwarded", consumed, forwarded)

    # Task 3.2e: summary output
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "topic": TOPIC_EVENTS,
        "total_messages_consumed": consumed,
        "event_counts": event_counts,
        "critical_escalations_forwarded": forwarded,
        "elapsed_seconds": round(elapsed, 2),
        "throughput_messages_per_second": round(consumed / elapsed, 2) if elapsed > 0 else None,
    }
    summary_path = os.path.join(OUTPUT_DIR, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info("Summary written to: %s", summary_path)

    return summary


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kafka assessment — producer/consumer")
    parser.add_argument("--mode", choices=["producer", "consumer", "both"], default="both")
    args = parser.parse_args()

    create_topics()

    if args.mode == "producer":
        producer = build_producer()
        run_producer(producer, EVENTS_FILE)

    elif args.mode == "consumer":
        consumer = build_consumer(TOPIC_EVENTS)
        run_consumer(consumer)

    elif args.mode == "both":
        producer = build_producer()
        run_producer(producer, EVENTS_FILE)

        consumer = build_consumer(TOPIC_EVENTS)
        summary = run_consumer(consumer)
        print("\nEvent summary:", json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
