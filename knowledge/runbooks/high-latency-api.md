# Runbook: High API Latency

**Anomaly Type:** LATENCY_SPIKE  
**Severity:** HIGH → CRITICAL  
**Services:** Any service (most common: payment-service, order-service, api-gateway)  
**Tags:** latency, performance, database, connection-pool

---

## Overview

A high latency incident occurs when p99 response times exceed 3× the SLA baseline for more than 2 consecutive minutes. This runbook covers the most common causes and investigation steps.

---

## Detection Criteria

- `service_latency_p99_ms` > 1500ms for 2+ minutes
- OR `service_latency_p99_ms` > 3000ms for any duration
- Often accompanied by increased error rates (timeout 5xx)

---

## Step 1: Quick Assessment (0–2 minutes)

1. Check which endpoints are affected:
   - High latency on ALL endpoints → infrastructure issue (database, CPU, network)
   - High latency on specific endpoint → application code or query issue
2. Check if a deployment happened in the last 60 minutes
3. Check CPU and Memory of the affected service
4. Check database connection pool utilization

---

## Step 2: Database Investigation (2–8 minutes)

The most common cause of latency spikes is database query degradation.

1. Check for slow queries:
   ```sql
   SELECT query, mean_exec_time, calls FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;
   ```
2. Check for table locks:
   ```sql
   SELECT * FROM pg_locks WHERE granted = false;
   ```
3. Check connection pool utilization — if at max, connections are queuing
4. Check if an index was recently dropped or a new query plan was chosen

**Action:** If slow queries found, kill long-running queries and investigate index coverage.

---

## Step 3: Connection Pool Investigation

1. If `service_db_connections` is at or near pool max:
   - Restart the affected service to release stale connections
   - Increase pool max if connections are legitimately needed
2. Check for connection leaks: connections opened but never closed

**Action:** `kubectl rollout restart deployment/{service}` or equivalent Docker restart.

---

## Step 4: Upstream Dependency Check

1. Check if downstream services called by this service are also degraded
2. If a dependency is slow, this service will inherit that latency
3. Enable circuit breaker if available to fail fast

**Action:** If dependency is degraded, implement fallback or cache response.

---

## Step 5: Deployment Rollback

If a deployment happened within the last 60 minutes AND latency started after deployment:

1. Identify the deployment: `kubectl rollout history deployment/{service}`
2. Rollback: `kubectl rollout undo deployment/{service}`
3. Monitor recovery — should see p99 drop within 2–3 minutes

---

## Step 6: Scaling (If Root Cause is Load)

If none of the above apply and traffic has genuinely increased:

1. Scale up: `kubectl scale deployment/{service} --replicas=N`
2. Check HPA settings — auto-scaling may not have triggered yet

---

## Resolution Criteria

- `service_latency_p99_ms` returns below 1.5× SLA for 5+ consecutive minutes
- No error rate anomaly remaining

---

## Escalation

- If not resolved within 30 minutes: escalate to {on-call-lead}
- If database issue: escalate to database team
- If deployment rollback doesn't help: escalate to owning service team
