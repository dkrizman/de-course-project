import docker
import pytest


@pytest.fixture(scope="session")
def ingest_image():
    client = docker.from_env()

    image, _ = client.images.build(
        path=".",
        tag="meridian-ingest-test",
    )

    return image.tags[0]