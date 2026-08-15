import os
from alembic.config import Config
from alembic.command import upgrade
from data_ingest.data_detection import ingest_to_bronze

def run_migrations():
    """Run Alembic migrations"""
    alembic_cfg = Config("alembic.ini")
    
    # Set database URL from environment (use psycopg driver, not psycopg2)
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Convert postgresql:// to postgresql+psycopg:// for psycopg v3
    if database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', 'postgresql+psycopg://', 1)
    
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    
    # Run migrations
    upgrade(alembic_cfg, "head")
    print("✓ Migrations completed")

def main():
    """Main entry point"""
    # print("Starting trip data ingestion...")
    
    # Run migrations first
    # print("Running database migrations...")
    # run_migrations()
    
    # Load and ingest data
    # region = os.getenv('REGION', 'NYC')  # Default region
    # target_month = os.getenv('TARGET_MONTH', '2024-01')

    layer = os.environ["LAYER"]
    job = os.environ["JOB"]
    window = os.environ["WINDOW"]

    FUNC_MAP = {
        "ingest-to-bronze": ingest_to_bronze,
        # silver and gold to be added later
    }
    
    print(f"Ingesting {job} data for {window}...")
    try:
        FUNC_MAP[layer](job.split(":")[1], window)
    except Exception as e:
        print(f"✗ Ingestion failed: {e}")
        raise
    print("✓ Ingestion complete")

if __name__ == "__main__":
    main()