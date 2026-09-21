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

inspect layer job window:
    docker compose --profile ingest run --rm --build \
        -e LAYER="{{layer}}" \
        -e JOB="{{job}}" \
        -e WINDOW="{{window}}" \
        ingest

report layer job station window:
    docker compose --profile ingest run --rm --build \
        -e LAYER="{{layer}}" \
        -e JOB="{{job}}" \
        -e STATION="{{station}}" \
        -e WINDOW="{{window}}" \
        ingest


run-pipeline market month:
    docker compose exec airflow airflow dags trigger ingest_trips \
        --conf '{"market": "{{market}}", "month": "{{month}}"}'