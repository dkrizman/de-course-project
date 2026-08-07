FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONDOWNWRITEBYTECODE=1

WORKDIR /app

RUN pip install --no-cache-dir "psycopg[binary]>=3.2" "alembic>=1.13"

COPY alembic.ini ./
COPY alembic ./alembic
COPY src ./src
COPY pyproject.toml ./

RUN pip install -e .

# CMD ["python", "-m", "trip_ingest", "/data/drops"]