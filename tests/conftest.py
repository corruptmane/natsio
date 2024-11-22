from typing import Generator, cast
import pytest
from testcontainers.nats import NatsContainer  # type: ignore[import-untyped]

from natsio.abc.json import JSONSerializerProto
from natsio.utils.json.native import JSONSerializer
from natsio.utils.json.orjson import ORJSONSerializer


@pytest.fixture(scope="session")
def nats_uri() -> Generator[list[str], None, None]:
    """Start a NATS server using testcontainers."""
    with NatsContainer() as container:
        nats_uri = container.nats_uri()
        yield [nats_uri]


@pytest.fixture(params=[JSONSerializer(), ORJSONSerializer()], ids=["json", "orjson"], scope="session")
def serializer(request: pytest.FixtureRequest) -> JSONSerializerProto:
    return cast(JSONSerializerProto, request.param)
