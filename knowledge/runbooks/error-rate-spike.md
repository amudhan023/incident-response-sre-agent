# Runbook: Error Rate Spike (5xx)

**Anomaly Type:** ERROR_RATE_SPIKE  
**Severity:** HIGH → CRITICAL  
**Services:** Any  
**Tags:** errors, 5xx, exceptions, circuit-breaker

---

## Overview

An error rate spike means a significant percentage of requests are returning 500-series responses. Root causes range from application bugs (code errors) to infrastructure issues (database unreachable, OOM).

---

## Detection Criteria

- `service_error_rate_percent` > 5% for 2+ minutes
- OR sudden spike > 20% for any duration

---

## Step 1: Classify the Error Type (0–3 minutes)

1. Check error logs for exception patterns
2. Are errors on ALL endpoints or specific ones?
3. What is the HTTP status code distribution?
   - 500: application exception
   - 502: upstream/proxy error
   - 503: service unavailable / circuit open
   - 504: gateway timeout

---

## Step 2: Application Exception Investigation

```bash
# Get error logs
kubectl logs -l app={service} --tail=100 | grep "ERROR\|Exception\|FATAL"

# Look for stack traces
kubectl logs -l app={service} --tail=200 | grep -A 10 "Exception"
```

Common causes:
- Null pointer / unhandled exception in new code
- Database error propagated as 500
- Invalid configuration after deployment

---

## Step 3: Deployment Correlation

If a deployment happened in the last 30 minutes:

1. Compare error rate timeline with deployment timestamp
2. If correlated: ROLLBACK immediately

```bash
kubectl rollout undo deployment/{service}
```

Monitor error rate — should drop within 2 minutes of rollback.

---

## Step 4: Downstream Dependency Check

If errors are timeout/connection-related (not logic errors):

1. Check all downstream services for degradation
2. Check database connectivity
3. Check if circuit breaker is open

If a dependency is down:
- Implement fallback response if possible
- Enable circuit breaker to fail fast
- Alert the owning team of the dependency

---

## Step 5: Traffic-Induced Errors

If no deployment and no dependency issue:

1. Check if request volume spike is causing resource exhaustion
2. Check thread pool exhaustion (all threads busy → rejecting requests)
3. Scale up if needed: `kubectl scale deployment/{service} --replicas=N`

---

## Resolution Criteria

- `service_error_rate_percent` < 1% for 5+ consecutive minutes
- Error log rate returning to baseline

---

## Escalation

- Errors > 20% for > 10 minutes: P0 escalation
- Revenue-impacting service (payment, order): immediate escalation
