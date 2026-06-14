# Runbook: Kafka Consumer Lag

**Anomaly Type:** KAFKA_CONSUMER_LAG  
**Severity:** HIGH  
**Services:** order-service, notification-service  
**Tags:** kafka, consumer-lag, messaging, backpressure

---

## Overview

Kafka consumer lag occurs when a consumer group cannot keep up with the message production rate. Growing lag means events are processed with increasing delay, causing business logic delays (order confirmation, notifications, etc.).

---

## Detection Criteria

- `kafka_consumer_lag` > 10,000 messages for 2+ minutes
- OR lag growing monotonically (not catching up)
- Often accompanied by notification delays or order processing delays

---

## Step 1: Identify the Lagging Consumer Group (0–2 minutes)

```bash
# List consumer groups
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list

# Show lag for specific group
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group order-processor --describe
```

Identify which partition(s) have the most lag.

---

## Step 2: Check Consumer Health

1. Is the consumer service running?
   - `kubectl get pods -l app=order-service`
2. Is the consumer in the group?
   - Check "CONSUMER-ID" column in the describe output — if empty, consumer left group
3. Check consumer logs for errors:
   - Processing errors, deserialization failures, downstream unavailability

---

## Step 3: Root Cause Analysis

### Cause A: Consumer Crashed / Left Group

**Symptoms:** Consumer ID absent from group describe; lag growing rapidly  
**Fix:** Restart consumer service; verify it rejoins the group

### Cause B: Consumer Too Slow (Processing Bottleneck)

**Symptoms:** Consumer is running but processing rate < production rate  
**Investigation:** Check CPU/memory of consumer; check if downstream calls are slow  
**Fix:**
- Scale up consumer replicas (add more partitions if needed)
- Optimize message processing (batch, async, cache)
- Temporarily increase `max.poll.interval.ms` if processing is legitimately slow

### Cause C: Kafka Rebalance Storm

**Symptoms:** Frequent rebalances; consumers joining/leaving repeatedly  
**Investigation:** Check `session.timeout.ms` vs `max.poll.interval.ms` settings  
**Fix:**
- Increase `session.timeout.ms` to 60000
- Decrease `max.poll.records` to process fewer messages per poll
- Use Static Group Membership (`group.instance.id`) to avoid unnecessary rebalances

### Cause D: Production Spike

**Symptoms:** Lag started when upstream traffic spiked  
**Fix:** Scale consumer replicas proportionally; ensure enough partitions exist

---

## Emergency Lag Reduction

If lag is extremely high (>500k) and needs rapid reduction:

```bash
# Skip to the end of the topic (WARNING: drops all pending messages)
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group order-processor --topic orders --reset-offsets --to-latest --execute
```

⚠️ **Only use offset reset if messages are idempotent or already processed elsewhere.**

---

## Scaling Consumer Replicas

```bash
kubectl scale deployment/order-service --replicas=6
```

Note: You can have at most as many consumers as partitions. Check partition count first.

---

## Resolution Criteria

- `kafka_consumer_lag` decreasing trend for 5+ minutes
- Lag < 1000 messages and continuing to decrease
