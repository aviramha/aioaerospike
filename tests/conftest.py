import uuid

import pytest

from aioaerospike.client import AerospikeClient


@pytest.fixture
async def client(scope="module"):
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()
    return client


@pytest.fixture
def namespace():
    return "test"


@pytest.fixture
def set_name():
    """
    Randomize name to avoid collision between tests
    """
    return uuid.uuid4().hex[:8]


@pytest.fixture
def key():
    """
    Randomize name to avoid collision between tests
    """
    return uuid.uuid4().hex[:8]
