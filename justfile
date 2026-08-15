# Start all containers
up:
    docker compose up -d

# Stop all containers
down:
    docker compose down

ingest:
    docker compose --profile ingest run --rm ingest

logs:
    docker compose logs -f ingest
