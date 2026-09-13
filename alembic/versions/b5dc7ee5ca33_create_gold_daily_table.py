"""create_gold_daily_table

Revision ID: b5dc7ee5ca33
Revises: 0fa2d5948517
Create Date: 2026-08-23 18:19:50.126099

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5dc7ee5ca33'
down_revision: Union[str, Sequence[str], None] = '0fa2d5948517'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE trips_gold_daily_station (
        RIDE_DAY date NOT NULL,
        STATION text NOT NULL,
        ARRIVALS bigint NOT NULL,
        DEPARTURES bigint NOT NULL,
        PRIMARY KEY (RIDE_DAY, STATION)
    )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE trips_gold_daily_station")
