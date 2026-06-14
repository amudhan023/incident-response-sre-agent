"""Kafka producer/consumer wrappers with retry logic."""
from __future__ import annotations
import json
import logging
import os
import time
from typing import Callable

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _wait_for_kafka(max_retries: int = 30, delay: float = 5.0) -> None:
    from confluent_kafka.admin import AdminClient
    for attempt in range(1, max_retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            meta = admin.list_topics(timeout=5)
            if meta:
                logger.info("Kafka is ready.")
                return
        except Exception as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            time.sleep(delay)
    raise RuntimeError("Kafka never became available.")


def make_producer() -> Producer:
    _wait_for_kafka()
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
    })


def publish(producer: Producer, topic: str, message: dict) -> None:
    def _cb(err, msg):
        if err:
            logger.error("Delivery failed for topic %s: %s", topic, err)

    producer.produce(topic, json.dumps(message).encode("utf-8"), callback=_cb)
    producer.poll(0)


def flush(producer: Producer) -> None:
    producer.flush(timeout=10)


def make_consumer(
    topics: list[str],
    group_id: str,
    auto_offset_reset: str = "latest",
    max_retries: int = 30,
) -> Consumer:
    _wait_for_kafka(max_retries=max_retries)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    })
    consumer.subscribe(topics)
    logger.info("Subscribed to topics: %s (group: %s)", topics, group_id)
    return consumer


def consume_loop(
    consumer: Consumer,
    handler: Callable[[dict], None],
    poll_timeout: float = 1.0,
) -> None:
    """Run a blocking consume loop, calling handler for each valid message."""
    try:
        while True:
            msg = consumer.poll(timeout=poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue
            try:
                value = json.loads(msg.value().decode("utf-8"))
                handler(value)
            except Exception as exc:
                logger.exception("Error handling message from %s: %s", msg.topic(), exc)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
