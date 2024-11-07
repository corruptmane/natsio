from typing import Generator
import pytest
from testcontainers.nats import NatsContainer  # type: ignore[import-untyped]


@pytest.fixture(scope="session")
def nats_uri() -> Generator[list[str], None, None]:
    """Start a NATS server using testcontainers."""
    with NatsContainer() as container:
        nats_uri = container.nats_uri()
        yield [nats_uri]
