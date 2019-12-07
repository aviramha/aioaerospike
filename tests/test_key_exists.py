import pytest


@pytest.mark.asyncio
async def test_key_exists(namespace, set_name, key, client):
    exists = await client.key_exists(namespace, set_name, key)
    assert not exists

    await client.put_key(
        namespace, set_name, key, {"bin_to_exists": "test_bin_value_to_exists"},
    )

    exists = await client.key_exists(namespace, set_name, key)
    assert exists
