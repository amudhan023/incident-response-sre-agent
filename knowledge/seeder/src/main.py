"""
Knowledge Seeder — one-shot job that populates Qdrant with:
  • incidents   (historical incident records, chunked into 4 semantic chunks each)
  • runbooks    (operational runbooks, chunked by section)
  • architecture (service documentation)
"""
from __future__ import annotations
import json
import logging
import os
import sys
import time
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance, VectorParams, PointStruct, PayloadSchemaType
)
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("knowledge-seeder")

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

VECTOR_DIM  = 384  # all-MiniLM-L6-v2
MODEL_NAME  = "all-MiniLM-L6-v2"

KNOWLEDGE_DIR = Path("/app/knowledge")

COLLECTIONS = {
    "incidents":    {"size": VECTOR_DIM, "distance": Distance.COSINE},
    "runbooks":     {"size": VECTOR_DIM, "distance": Distance.COSINE},
    "architecture": {"size": VECTOR_DIM, "distance": Distance.COSINE},
}


def wait_for_qdrant(client: QdrantClient, max_retries: int = 20) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            client.get_collections()
            logger.info("Qdrant is ready.")
            return
        except Exception as exc:
            logger.warning("Qdrant not ready (%d/%d): %s", attempt, max_retries, exc)
            time.sleep(3)
    raise RuntimeError("Qdrant never became available.")


def ensure_collections(client: QdrantClient) -> None:
    existing = {c.name for c in client.get_collections().collections}
    for name, cfg in COLLECTIONS.items():
        if name in existing:
            logger.info("Collection '%s' already exists — skipping.", name)
            continue
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=cfg["size"], distance=cfg["distance"]),
        )
        logger.info("Created collection '%s'.", name)


def embed(model: SentenceTransformer, texts: list[str]) -> list[list[float]]:
    return model.encode(texts, show_progress_bar=False).tolist()


# ─── Incidents ────────────────────────────────────────────────────────────────

def seed_incidents(client: QdrantClient, model: SentenceTransformer) -> None:
    path = KNOWLEDGE_DIR / "incidents" / "incidents.json"
    if not path.exists():
        logger.warning("incidents.json not found — skipping.")
        return

    incidents = json.loads(path.read_text())
    points: list[PointStruct] = []
    point_id = 1

    for inc in incidents:
        chunks = [
            {
                "chunk_type": "summary",
                "text": f"{inc['title']}. Affected: {', '.join(inc['affected_services'])}. {inc.get('symptoms','')}",
            },
            {
                "chunk_type": "root_cause",
                "text": f"Root cause: {inc.get('root_cause','')}. Category: {inc.get('root_cause_category','')}.",
            },
            {
                "chunk_type": "resolution",
                "text": f"Resolution: {inc.get('resolution','')}. MTTR: {inc.get('mttr_minutes',0)} minutes.",
            },
            {
                "chunk_type": "factors",
                "text": f"Contributing factors: {'. '.join(inc.get('contributing_factors',[]))}. Tags: {', '.join(inc.get('tags',[]))}.",
            },
        ]

        texts = [c["text"] for c in chunks]
        embeddings = embed(model, texts)

        for chunk, vec in zip(chunks, embeddings):
            points.append(PointStruct(
                id=point_id,
                vector=vec,
                payload={
                    "incident_id":          inc["incident_id"],
                    "chunk_type":           chunk["chunk_type"],
                    "text":                 chunk["text"],
                    "title":                inc["title"],
                    "anomaly_type":         inc["anomaly_type"],
                    "severity":             inc["severity"],
                    "affected_services":    inc["affected_services"],
                    "root_cause_category":  inc.get("root_cause_category",""),
                    "mttr_minutes":         inc.get("mttr_minutes", 0),
                    "deployment_correlated":inc.get("deployment_correlated", False),
                    "tags":                 inc.get("tags", []),
                    "environment":          inc.get("environment","production"),
                    "resolved":             True,
                },
            ))
            point_id += 1

    client.upsert(collection_name="incidents", points=points)
    logger.info("Seeded %d incident chunks from %d incidents.", len(points), len(incidents))


# ─── Runbooks ─────────────────────────────────────────────────────────────────

RUNBOOK_META = {
    "high-latency-api.md":               {"anomaly_types": ["LATENCY_SPIKE"], "tags": ["latency","database","connection-pool"]},
    "database-connection-exhaustion.md":  {"anomaly_types": ["DB_CONNECTION_EXHAUSTION","LATENCY_SPIKE"], "tags": ["database","postgres","connection-pool"]},
    "kafka-consumer-lag.md":             {"anomaly_types": ["KAFKA_CONSUMER_LAG"], "tags": ["kafka","consumer-lag","messaging"]},
    "memory-leak.md":                    {"anomaly_types": ["MEMORY_LEAK"], "tags": ["memory","oom","heap","leak"]},
    "cpu-saturation.md":                 {"anomaly_types": ["CPU_SATURATION"], "tags": ["cpu","performance","scaling"]},
    "error-rate-spike.md":               {"anomaly_types": ["ERROR_RATE_SPIKE","DEPLOYMENT_FAILURE"], "tags": ["errors","5xx","exceptions"]},
    "deployment-rollback.md":            {"anomaly_types": ["DEPLOYMENT_FAILURE","ERROR_RATE_SPIKE","LATENCY_SPIKE"], "tags": ["deployment","rollback","release"]},
}


def _split_sections(content: str, filename: str) -> list[dict]:
    """Split markdown into sections by ## headers, keeping frontmatter as overview."""
    chunks = []
    current_title = "Overview"
    current_lines: list[str] = []

    for line in content.split("\n"):
        if line.startswith("## "):
            if current_lines:
                chunks.append({"title": current_title, "text": "\n".join(current_lines).strip()})
            current_title = line[3:].strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_lines:
        chunks.append({"title": current_title, "text": "\n".join(current_lines).strip()})

    return [c for c in chunks if len(c["text"]) > 30]


def seed_runbooks(client: QdrantClient, model: SentenceTransformer) -> None:
    runbook_dir = KNOWLEDGE_DIR / "runbooks"
    points: list[PointStruct] = []
    point_id = 10000

    for md_file in sorted(runbook_dir.glob("*.md")):
        meta = RUNBOOK_META.get(md_file.name, {"anomaly_types": [], "tags": []})
        content = md_file.read_text()
        sections = _split_sections(content, md_file.name)

        texts = [f"{s['title']}: {s['text'][:500]}" for s in sections]
        if not texts:
            continue

        embeddings = embed(model, texts)
        for section, vec in zip(sections, embeddings):
            points.append(PointStruct(
                id=point_id,
                vector=vec,
                payload={
                    "runbook_id":    md_file.stem,
                    "filename":      md_file.name,
                    "section_title": section["title"],
                    "text":          section["text"][:1000],
                    "anomaly_types": meta["anomaly_types"],
                    "tags":          meta["tags"],
                },
            ))
            point_id += 1

    client.upsert(collection_name="runbooks", points=points)
    logger.info("Seeded %d runbook chunks.", len(points))


# ─── Architecture ─────────────────────────────────────────────────────────────

def seed_architecture(client: QdrantClient, model: SentenceTransformer) -> None:
    path = KNOWLEDGE_DIR / "architecture" / "services.json"
    if not path.exists():
        logger.warning("services.json not found — skipping.")
        return

    data = json.loads(path.read_text())
    points: list[PointStruct] = []
    point_id = 20000

    for svc in data["services"]:
        full_desc = (
            f"{svc['name']}: {svc['description']} "
            f"Team: {svc['team']}. Criticality: {svc['criticality']}. "
            f"Language: {svc['language']}. "
            f"Depends on: {', '.join(svc['dependencies_downstream'])}. "
            f"Called by: {', '.join(svc['dependencies_upstream'])}. "
            f"Common failures: {', '.join(svc['common_failure_modes'])}."
        )
        vec = embed(model, [full_desc])[0]
        points.append(PointStruct(
            id=point_id,
            vector=vec,
            payload={
                "service_name":          svc["name"],
                "description":           svc["description"],
                "team":                  svc["team"],
                "criticality":           svc["criticality"],
                "language":              svc["language"],
                "sla":                   svc["sla"],
                "dependencies_upstream": svc["dependencies_upstream"],
                "dependencies_downstream": svc["dependencies_downstream"],
                "common_failure_modes":  svc["common_failure_modes"],
                "on_call":               svc["on_call"],
                "runbooks":              svc["runbooks"],
                "text":                  full_desc,
            },
        ))
        point_id += 1

    client.upsert(collection_name="architecture", points=points)
    logger.info("Seeded %d service architecture docs.", len(points))


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Knowledge Seeder starting")
    logger.info("=" * 60)

    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    wait_for_qdrant(client)
    ensure_collections(client)

    logger.info("Loading embedding model: %s", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)
    logger.info("Embedding model loaded.")

    seed_incidents(client, model)
    seed_runbooks(client, model)
    seed_architecture(client, model)

    # Print collection stats
    for name in COLLECTIONS:
        info = client.get_collection(name)
        logger.info("Collection '%s': %d vectors", name, info.vectors_count)

    logger.info("Knowledge seeding complete ✅")


if __name__ == "__main__":
    main()
