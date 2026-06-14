# Runbook: Database Connection Pool Exhaustion

**Anomaly Type:** DB_CONNECTION_EXHAUSTION  
**Severity:** CRITICAL  
**Services:** payment-service, order-service, inventory-service  
**Tags:** database, postgres, connection-pool, exhaustion

---

## Overview

Connection pool exhaustion occurs when all database connections in the pool are held by active requests, causing new requests to queue and eventually timeout. This cascades into high error rates and latency spikes.

---

## Detection Criteria

- `service_db_connections` ≥ 95% of pool max
- AND `service_error_rate_percent` > 5% (connections timing out)
- AND `service_latency_p99_ms` spike (requests waiting for connections)

---

## Immediate Actions (0–3 minutes)

### 1. Identify connection holders

```sql
SELECT pid, usename, application_name, state, query_start, query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY query_start ASC;
```

### 2. Kill longest-running idle connections

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state = 'idle'
AND query_start < NOW() - INTERVAL '5 minutes';
```

### 3. Check for connection leaks

```sql
SELECT application_name, COUNT(*) as connections
FROM pg_stat_activity
GROUP BY application_name
ORDER BY connections DESC;
```

If one service has far more connections than expected, it has a connection leak.

---

## Root Cause Investigation

### Cause A: Connection Leak

**Symptoms:** Connection count grows monotonically over time, never decreases  
**Fix:** Restart affected service to release connections; fix code to use connection pooling correctly (use `with` statements, context managers)

### Cause B: Slow Queries Holding Connections

**Symptoms:** Connections are in 'active' state executing slow queries  
**Fix:** Kill slow queries, add missing indexes, optimize query plan

```sql
SELECT pg_cancel_backend(pid)
FROM pg_stat_activity
WHERE state = 'active'
AND query_start < NOW() - INTERVAL '30 seconds';
```

### Cause C: Traffic Surge

**Symptoms:** All connections legitimately needed; higher than normal request volume  
**Fix:** Increase pool size temporarily; scale up service replicas; add read replicas

### Cause D: PgBouncer / Proxy Misconfiguration

**Symptoms:** Pool exhaustion despite low actual database load  
**Fix:** Check PgBouncer configuration; restart PgBouncer if needed

---

## Emergency Restart Procedure

If connections cannot be freed and errors are escalating:

```bash
# Restart the affected service (loses in-flight requests)
kubectl rollout restart deployment/payment-service

# Or for Docker:
docker restart payment-service
```

After restart, monitor connection count — should drop immediately.

---

## Preventative Measures

1. Set `pool_timeout` in application config to fail fast (not queue forever)
2. Enable connection pool metrics alerting at 80% utilization
3. Use PgBouncer transaction-mode pooling to reduce connection count
4. Set `idle_in_transaction_session_timeout = 30s` in Postgres

---

## Resolution Criteria

- `service_db_connections` < 70% of pool max for 3+ minutes
- `service_error_rate_percent` returns below 1%
