# Runbook: CPU Saturation

**Anomaly Type:** CPU_SATURATION  
**Severity:** HIGH  
**Services:** api-gateway, payment-service  
**Tags:** cpu, performance, scaling, infinite-loop

---

## Overview

CPU saturation occurs when a service is CPU-bound and cannot process requests fast enough. At >90% CPU, request processing degrades significantly and latency climbs.

---

## Detection Criteria

- `service_cpu_percent` > 85% for 3+ consecutive minutes
- Often accompanied by latency increase and dropped requests

---

## Step 1: Identify CPU Consumer (0–3 minutes)

```bash
# Top CPU processes in the container
kubectl exec -it {pod} -- top -b -n 1 | head -20

# Or for profiling:
kubectl exec -it {pod} -- python -c "import cProfile; ..."
```

Identify: is CPU consumed by one goroutine/thread (infinite loop) or all (overload)?

---

## Step 2: Root Cause Analysis

### Cause A: Traffic Overload (Legitimate)

**Symptoms:** CPU proportional to request rate; all threads active  
**Fix:** Scale up immediately — `kubectl scale deployment/api-gateway --replicas=6`

### Cause B: Infinite Loop / Runaway Process

**Symptoms:** 100% CPU from one thread; request rate normal or low  
**Fix:** Identify the loop via profiling; restart service if loop cannot be broken

### Cause C: Expensive Computation / Regex

**Symptoms:** Specific endpoint causing CPU spike; often regex ReDoS  
**Fix:** Rate limit the expensive endpoint; rewrite problematic regex; add async offloading

### Cause D: Garbage Collection Storm

**Symptoms:** GC CPU > 30%; heap nearly full; short-lived objects  
**Fix:** Increase heap size; tune GC parameters; reduce object allocation

---

## Immediate Remediation

### Scale Out

```bash
kubectl scale deployment/api-gateway --replicas=8
```

### Identify and Kill Runaway Process

```bash
# Find high-CPU threads
kubectl exec -it {pod} -- ps aux --sort=-%cpu | head -5

# For Java: get thread dump
kubectl exec -it {pod} -- kill -3 {pid}
```

### Enable CPU Limits (Emergency)

Enforce a CPU limit to prevent one service from starving others:

```yaml
resources:
  limits:
    cpu: "2"
```

---

## Step 3: Traffic Analysis

1. Is there an abnormal traffic spike?
2. Is a specific client hammering the service?
3. Implement rate limiting if needed:
   - IP-based rate limiting at api-gateway
   - Per-user rate limiting at application layer

---

## Resolution Criteria

- `service_cpu_percent` < 70% for 5+ minutes
- `service_latency_p99_ms` returning to baseline
