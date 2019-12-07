import pytest

from aioaerospike.client import AerospikeClient


@pytest.mark.asyncio
async def test_sanity_put_get():
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()

    await client.put_key(
        "test", "test_set", "test_key", "test_bin_name", "test_value"
    )

    result = await client.get_key("test", "test_set", "test_key")
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_bytes_key():
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()

    await client.put_key(
        "test", "test_set", b"test_bytes_key", "test_bin_name", "test_value"
    )

    result = await client.get_key("test", "test_set", b"test_bytes_key")
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_integer_key():
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()

    await client.put_key("test", "test_set", 300, "test_bin_name", "test_value")

    result = await client.get_key("test", "test_set", 300)
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_double_key():
    client = AerospikeClient("127.0.0.1", "admin", "admin", port=3000)
    await client.connect()

    await client.put_key(
        "test", "test_set", 123.123, "test_bin_name", "test_value"
    )

    result = await client.get_key("test", "test_set", 123.123)
    assert result["test_bin_name"] == "test_value"
