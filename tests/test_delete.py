import pytest


@pytest.mark.asyncio
async def test_delete_key(client):
    await client.put_key(
        "test",
        "test_delete_set",
        "test_delete_key",
        "bin_to_delete",
        "test_bin_value_to_delete",
    )

    result = await client.get_key("test", "test_delete_set", "test_delete_key")
    assert result["bin_to_delete"] == "test_bin_value_to_delete"

    await client.delete_key("test", "test_delete_set", "test_delete_key")

    result = await client.get_key("test", "test_delete_set", "test_delete_key")
    assert len(result) == 0
