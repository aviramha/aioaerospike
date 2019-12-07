import pytest


@pytest.mark.asyncio
async def test_key_exists(client):
    exists = await client.key_exists(
        "test", "test_exists_set", "test_key_exists"
    )
    assert not exists

    await client.put_key(
        "test",
        "test_exists_set",
        "test_key_exists",
        "bin_to_exists",
        "test_bin_value_to_exists",
    )

    exists = await client.key_exists(
        "test", "test_exists_set", "test_key_exists"
    )
    assert exists
