import pytest


@pytest.mark.asyncio
async def test_delete_key(namespace, set_name, key, client):
    await client.put_key(
        namespace, set_name, key, {"bin_to_delete": "test_bin_value_to_delete"}
    )

    result = await client.get_key(namespace, set_name, key)
    assert result["bin_to_delete"] == "test_bin_value_to_delete"

    await client.delete_key(namespace, set_name, key)

    result = await client.get_key(namespace, set_name, key)
    assert len(result) == 0
