# Runbook: Memory Leak / OOM

**Anomaly Type:** MEMORY_LEAK  
**Severity:** HIGH → CRITICAL (when OOM imminent)  
**Services:** notification-service, payment-service  
**Tags:** memory, oom, heap, gc, jvm, leak

---

## Overview

Memory leaks manifest as monotonically growing memory usage that never plateaus. Left unchecked, they result in OOM kills, service restarts, and data loss for in-flight requests.

---

## Detection Criteria

- `service_memory_percent` growing by > 5% over 10 minutes without plateau
- OR `service_memory_percent` > 85%
- GC frequency increasing without freeing sufficient memory

---

## Immediate Actions (Memory > 90%)

**Trigger an immediate controlled restart if memory exceeds 90%:**

```bash
kubectl rollout restart deployment/notification-service
```

A controlled restart is better than an OOM kill — it gracefully handles in-flight requests.

---

## Step 1: Determine Leak Type (5–10 minutes)

### JVM/Java Service

1. Generate heap dump BEFORE restart:
   ```bash
   kubectl exec -it {pod} -- jmap -dump:format=b,file=/tmp/heap.bin {pid}
   kubectl cp {pod}:/tmp/heap.bin ./heap.bin
   ```
2. Analyze with Eclipse MAT or VisualVM
3. Look for: large object count, retained heap, GC roots

### Python Service

1. Check memory profiler (if instrumented):
   ```bash
   kubectl exec -it {pod} -- python -c "import tracemalloc; ..."
   ```
2. Common Python leak sources: unbounded caches, global lists, circular references

### Go Service

1. Enable pprof endpoint and capture:
   ```bash
   curl http://{service}:6060/debug/pprof/heap > heap.prof
   go tool pprof heap.prof
   ```

---

## Common Root Causes

### Unbounded In-Memory Cache

**Symptoms:** Memory grows proportionally to distinct users/keys  
**Fix:** Add LRU eviction with a maximum size; use Redis for shared caching

### Connection / Resource Leak

**Symptoms:** File descriptors or network connections growing  
**Check:** `lsof -p {pid} | wc -l` for file descriptor count  
**Fix:** Ensure all connections use context managers / `with` blocks; add `finally` blocks

### Large Request/Response Buffering

**Symptoms:** Memory spikes during high traffic  
**Fix:** Stream large responses instead of buffering in memory

### Event Listener Accumulation

**Symptoms:** Memory grows proportionally to events registered  
**Fix:** Ensure event listeners are deregistered when no longer needed

---

## Step 2: Temporary Mitigation

Configure memory limits and restart policy:

```yaml
resources:
  limits:
    memory: 2Gi
  requests:
    memory: 512Mi
```

Set restart policy to auto-restart at 90% memory:
```bash
# Add liveness probe that checks memory and fails when too high
```

---

## Step 3: Long-Term Fix

1. Code review for resource management
2. Add memory profiling to CI pipeline
3. Set heap size limits (`-Xmx` for JVM)
4. Add automatic heap dump on OOM for future debugging

---

## Resolution Criteria

- `service_memory_percent` stable or decreasing for 10+ minutes
- No OOM events in last 15 minutes
