"""create silver table

Revision ID: 0fa2d5948517
Revises: 
Create Date: 2026-08-08 19:45:40.999382

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0fa2d5948517'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE trips_silver (
    ride_id text PRIMARY KEY,
    start_station_id text NOT NULL,
    end_station_id text NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    -- rideable_type text NOT NULL,
    start_station_name text,
    end_station_name text
    -- start_lat double precision,
    -- start_lng double precision,
    -- end_lat double precision,
    -- end_lng double precision,
    -- member_casual text
    )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE trips_silver")
