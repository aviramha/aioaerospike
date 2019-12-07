import pytest

from aioaerospike.client import AerospikeClient


@pytest.fixture
async def client(scope="module"):
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()
    return client
