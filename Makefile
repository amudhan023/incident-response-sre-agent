.PHONY: demo start stop clean logs status build seed help

# ─── Primary entry points ──────────────────────────────────────────────────────

demo: check-env
	@echo "🚀 Starting Incident Response SRE Agent demo..."
	@echo "   This will take ~3 minutes to fully initialize."
	@echo ""
	docker compose up --build -d
	@echo ""
	@echo "✅ All services starting. Run 'make status' to check health."
	@echo ""
	@echo "📍 Access points (ready in ~3 minutes):"
	@echo "   SRE Dashboard:  http://localhost:8000"
	@echo "   Grafana:        http://localhost:3000  (admin/admin)"
	@echo "   Kafka UI:       http://localhost:8080"
	@echo "   Mailhog:        http://localhost:8025"
	@echo "   Prometheus:     http://localhost:9090"
	@echo "   Qdrant:         http://localhost:6333/dashboard"
	@echo ""
	@echo "📧 Watch emails appear at http://localhost:8025 during incidents."
	@echo "   First failure injected ~1 minute after simulator starts."
	@echo ""
	@echo "📊 Run 'make logs' to follow agent activity."

start: check-env
	docker compose up -d

stop:
	docker compose down

clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# ─── Development helpers ───────────────────────────────────────────────────────

build:
	docker compose build

logs:
	docker compose logs -f detection-agent correlation-agent investigation-agent \
	    remediation-agent communication-agent postmortem-agent

logs-all:
	docker compose logs -f

logs-sim:
	docker compose logs -f event-simulator

logs-api:
	docker compose logs -f sre-api

status:
	@echo "=== Service Health ==="
	docker compose ps

seed:
	docker compose run --rm knowledge-seeder

restart-agents:
	docker compose restart detection-agent correlation-agent investigation-agent \
	    remediation-agent communication-agent postmortem-agent

# ─── Shortcuts ────────────────────────────────────────────────────────────────

check-env:
	@if [ -z "$(ANTHROPIC_API_KEY)" ]; then \
	    if [ ! -f .env ]; then \
	        echo "❌ No .env file found. Copy .env.example and add your ANTHROPIC_API_KEY:"; \
	        echo "   cp .env.example .env && edit .env"; \
	        exit 1; \
	    fi; \
	fi

help:
	@echo "Incident Response SRE Agent — Make targets"
	@echo ""
	@echo "  make demo    Build and start everything (primary command)"
	@echo "  make start   Start without rebuilding"
	@echo "  make stop    Stop all services"
	@echo "  make clean   Stop and remove all volumes"
	@echo "  make logs    Follow agent logs"
	@echo "  make status  Show service health"
	@echo "  make build   Build all Docker images"
