import docker
import pytest
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def ingest_image():
    client = docker.from_env()
    image, _ = client.images.build(path=".", tag="meridian-ingest-test")
    return image.tags[0]


@pytest.fixture
def network():
    with Network() as net:
        yield net


@pytest.fixture
def silver_db(network):
    # driver=None keeps get_connection_url() psycopg2/3-agnostic (plain postgresql://)
    pg = PostgresContainer("postgres:16", driver=None).with_network(network).with_network_aliases("db")
    with pg:
        yield pg