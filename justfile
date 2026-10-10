up:
    docker compose up -d --build
    @until curl -fsS http://localhost:8080/api/v2/monitor/health >/dev/null; do \
        echo "Waiting for Airflow to become ready..."; \
        sleep 2; \
    done
    docker compose exec airflow airflow dags unpause pipeline_controller && \
    docker compose exec airflow airflow dags unpause ingest_jc && \
    docker compose exec airflow airflow dags unpause ingest_nyc

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

run-pipeline market start_month="" end_month="":
    if [ -z "{{end_month}}" ]; then \
        docker compose exec airflow airflow dags trigger pipeline_controller \
            --conf '{"market": "{{market}}", "start_month": "{{start_month}}", "end_month": "{{start_month}}"}'; \
    else \
        docker compose exec airflow airflow dags trigger pipeline_controller \
            --conf '{"market": "{{market}}", "start_month": "{{start_month}}", "end_month": "{{end_month}}"}'; \
    fi

inspect-pipeline command job:
    docker compose --profile ingest run --rm --build \
        -e COMMAND="{{command}}" \
        -e JOB="{{job}}" \
        ingest

schedule:
    docker compose exec airflow airflow dags unpause schedule_jc_pipeline && \
    docker compose exec airflow airflow dags unpause schedule_nyc_pipeline