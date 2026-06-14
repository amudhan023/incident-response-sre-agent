"""Redis client wrapper with retry."""
from __future__ import annotations
import json
import logging
import os
import time

import redis

logger = logging.getLogger(__name__)

_client: redis.Redis | None = None


def get_client(max_retries: int = 20) -> redis.Redis:
    global _client
    if _client is not None:
        return _client
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))
    for attempt in range(1, max_retries + 1):
        try:
            r = redis.Redis(host=host, port=port, decode_responses=True)
            r.ping()
            _client = r
            logger.info("Redis connected at %s:%s", host, port)
            return _client
        except Exception as exc:
            logger.warning("Redis not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            time.sleep(2)
    raise RuntimeError("Redis never became available.")


def set_dedup(key: str, ttl_seconds: int) -> bool:
    """Set dedup key. Returns True if key was new (not duplicate), False if duplicate."""
    r = get_client()
    return bool(r.set(key, "1", nx=True, ex=ttl_seconds))


def push_metric(service: str, metric: str, value: float, max_len: int = 100) -> None:
    r = get_client()
    key = f"metric:{service}:{metric}"
    r.lpush(key, str(value))
    r.ltrim(key, 0, max_len - 1)
    r.expire(key, 3600)


def get_metric_history(service: str, metric: str, count: int = 50) -> list[float]:
    r = get_client()
    key = f"metric:{service}:{metric}"
    vals = r.lrange(key, 0, count - 1)
    return [float(v) for v in vals]


def set_json(key: str, value: dict, ttl: int = 3600) -> None:
    get_client().set(key, json.dumps(value), ex=ttl)


def get_json(key: str) -> dict | None:
    val = get_client().get(key)
    return json.loads(val) if val else None


def set_str(key: str, value: str, ttl: int = 3600) -> None:
    get_client().set(key, value, ex=ttl)


def get_str(key: str) -> str | None:
    return get_client().get(key)


def incr(key: str, ttl: int = 3600) -> int:
    r = get_client()
    val = r.incr(key)
    r.expire(key, ttl)
    return val
