"""Where the database is. Given — do not edit.

One process, two possible answers, and the difference is the whole of Task 8.

* From **your shell**, Postgres is a port your compose file published on your laptop:
  `localhost:55432`. That is the default below, and it is what the tests connect to.
* From **inside the compose network**, `localhost` is the ingest container itself — there is no
  Postgres there. The database is another container, reachable by its service name: `db:5432`.

So the container gets its DSN from the environment, and the environment is set in
`docker-compose.yml`. Nothing here changes.
"""
from __future__ import annotations

import os

# Non-default port, user, password and database — all four on purpose. Making this exact string
# work is Task 1.
DEFAULT_DSN = "postgresql://meridian:meridian@localhost:55432/meridian_trips"
SERVICE_DSN = "postgresql://meridian:meridian@db:5432/meridian_trips"



def database_url() -> str:
    return os.environ.get("DATABASE_URL", SERVICE_DSN)
