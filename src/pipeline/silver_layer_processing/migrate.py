"""Bring the database to the newest revision, from Python. Given — do not edit.

Production applies migrations the same way the tests do and the same way the container does: by
running the revisions you wrote. There is no second description of the schema anywhere.
"""
from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.command import upgrade

ROOT = Path(__file__).resolve().parents[2]


def _config() -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    return cfg


def upgrade_to_head() -> None:
    command.upgrade(_config(), "head")


def downgrade_to_base() -> None:
    command.downgrade(_config(), "base")


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