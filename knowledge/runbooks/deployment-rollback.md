# Runbook: Deployment Rollback

**Anomaly Type:** DEPLOYMENT_FAILURE  
**Severity:** HIGH → CRITICAL  
**Services:** Any  
**Tags:** deployment, rollback, canary, release

---

## Overview

A deployment failure occurs when a new release introduces regressions — error rate spikes, latency increases, startup failures, or health check failures shortly after deployment.

---

## Detection Criteria

- `service_error_rate_percent` spikes within 15 minutes of a deployment event
- OR `service_latency_p99_ms` increases > 2× after deployment
- OR service fails health checks after deployment

---

## Immediate Decision: Rollback vs. Forward Fix

**Rollback when:**
- Error rate > 5% and rising
- Customer-facing impact is immediate
- Root cause is unclear or complex
- Time to fix > 15 minutes

**Fix forward when:**
- Error rate < 2% and stable
- Fix is a 1-line config change
- Rollback would also break something (migration applied)

**Default: ROLLBACK.** The safest path is rollback first, investigate later.

---

## Step 1: Standard Kubernetes Rollback (< 2 minutes)

```bash
# Check current rollout status
kubectl rollout status deployment/{service}

# View rollout history
kubectl rollout history deployment/{service}

# Rollback to previous version
kubectl rollout undo deployment/{service}

# Monitor rollback
kubectl rollout status deployment/{service} --watch
```

---

## Step 2: Verify Rollback Success

```bash
# Confirm new replica set is active
kubectl get replicasets -l app={service}

# Check error rate returning to baseline
# Watch Grafana dashboard for {service}_error_rate_percent
```

Error rate should start dropping within 2 minutes and reach baseline within 5 minutes.

---

## Step 3: Docker Compose Rollback (local/dev)

```bash
# Pull previous image tag
docker pull {image}:{previous-tag}

# Update compose and restart
docker compose up -d --no-deps {service}
```

---

## Step 4: Database Migration Complications

If the deployment included a database migration that cannot be reversed:

1. DO NOT rollback the code — it would break against the migrated schema
2. Instead, apply a hotfix to work with the new schema
3. Or add a compatibility layer temporarily

---

## Step 5: Feature Flag Disable (if applicable)

If the regression is isolated to a specific feature:

1. Disable the feature flag for the new feature
2. Verify error rate drops
3. This is faster than a full rollback

---

## Post-Rollback Actions

1. Notify the deploying engineer
2. Lock the problematic version from being deployed again
3. Schedule a post-deployment review
4. Add automated tests that would have caught the regression
5. Write a lightweight incident report

---

## Resolution Criteria

- Error rate returns to pre-deployment baseline
- Latency returns to pre-deployment baseline
- Health checks all passing
