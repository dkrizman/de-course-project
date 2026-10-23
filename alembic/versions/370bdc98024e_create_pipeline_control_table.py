"""create pipeline control table

Revision ID: 370bdc98024e
Revises: b5dc7ee5ca33
Create Date: 2026-09-27 19:12:14.824844

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '370bdc98024e'
down_revision: Union[str, Sequence[str], None] = 'b5dc7ee5ca33'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE pipeline_control (
            PIPELINE_NAME text NOT NULL,
            market text NOT NULL,
            month text NOT NULL,
            layer text NOT NULL,
            completed_at timestamptz NOT NULL,
            tries integer NOT NULL,
            status text NOT NULL,
            days integer,
            failed_days text[],
            PRIMARY KEY (PIPELINE_NAME, market, month, layer)
        )
        """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TABLE pipeline_control")
