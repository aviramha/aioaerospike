
from aioaerospike.client import AerospikeClient
import pytest

@pytest.mark.asyncio
async def test_sanity_put_get():
    client = AerospikeClient('127.0.0.1', "admin", "admin", port=32778)
    await client.connect()

    await client.put_key("test", "test_set", "test_key", "test_bin_name", "test_value")

    result = await client.get_key("test", "test_set", "test_key")
    assert result["test_bin_name"] == "test_value"
