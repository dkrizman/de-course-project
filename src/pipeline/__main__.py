import os
from alembic.config import Config
from alembic.command import upgrade
from pipeline.data_ingest.data_detection import ingest_to_bronze
from pipeline.silver_layer_processing.ingest import inspect_silver, transform_to_silver
from pipeline.silver_layer_processing.migrate import run_migrations
import psycopg
from pipeline.silver_layer_processing.settings import database_url
from pipeline.data_ingest.data_detection import inspect_bronze
from pipeline.gold_layer_processing.silver_to_gold import transform_to_gold
from pipeline.gold_layer_processing.inspect_gold import inspect_gold

def main():
    """Main entry point"""
    # print("Starting trip data ingestion...")
    
    # Run migrations first
    print("Running database migrations...")
    run_migrations()

    layer = os.environ["LAYER"]
    job = os.environ["JOB"]
    window = os.environ["WINDOW"]
    if ":" in job:
        region = job.split(":")[1]
    else:
        region = job
    station = os.environ.get("STATION")

    try:
        if layer == "ingest-to-bronze":
            ingest_to_bronze(region, window)
        elif layer == "transform-to-silver":
            with psycopg.connect(database_url()) as conn:
                transform_to_silver(conn, region, window)
        elif layer == "bronze":
            inspect_bronze(region, window)
        elif layer == "silver":
            inspect_silver(region, window)
        elif layer == "transform-to-gold":
            with psycopg.connect(database_url()) as conn:
                transform_to_gold(conn, window)
        elif layer == "daily-station-trips":
            with psycopg.connect(database_url()) as conn:
                inspect_gold(conn, window, station, region)
        else:
            raise ValueError(f"Unknown layer: {layer}")
    except Exception as e:
        print(f"✗ Ingestion failed: {e}")
        raise
    print("✓ Ingestion complete")

if __name__ == "__main__":
    main()