up:
    docker compose up -d --build

down:
    docker compose down -v

# ingest:
#     docker compose --profile ingest run --rm ingest

run layer job window:
    docker compose --profile ingest run --rm --build \
        -e LAYER="{{layer}}" \
        -e JOB="{{job}}" \
        -e WINDOW="{{window}}" \
        ingest
