import pytest


@pytest.mark.asyncio
async def test_sanity_put_get(namespace, set_name, key, client):
    await client.put_key(
        namespace, set_name, key, {"test_bin_name": "test_value"}
    )

    result = await client.get_key(namespace, set_name, key)
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_bytes_key(namespace, set_name, client):
    await client.put_key(
        "test", set_name, b"test_bytes_key", {"test_bin_name": "test_value"}
    )

    result = await client.get_key("test", set_name, b"test_bytes_key")
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_integer_key(namespace, set_name, client):

    await client.put_key("test", set_name, 300, {"test_bin_name": "test_value"})

    result = await client.get_key("test", set_name, 300)
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
async def test_put_get_double_key(namespace, set_name, client):
    await client.put_key(
        "test", set_name, 123.123, {"test_bin_name": "test_value"}
    )

    result = await client.get_key("test", set_name, 123.123)
    assert result["test_bin_name"] == "test_value"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_input",
    [
        ["asd"],
        ["asd", 123, 123.123, ["lol"], b"noope", ["123", ["123", ["123"]]]],
        ["testlong list" * 200],
        ["a"] * 60000,
    ],
)
async def test_put_get_list(test_input, namespace, set_name, key, client):
    await client.put_key(namespace, set_name, key, {"test_bin": test_input})
    result = await client.get_key(namespace, set_name, key)
    assert result["test_bin"] == test_input


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_input",
    [
        {"lol": "rofl"},
        {"test1": {"test2": {"test": "test3"}}},
        {1: {123.123: {b"asdsd": ["asd", "asd"]}}},
        {"longkey" * 4000: ["l"] * 60000},
    ],
)
async def test_put_get_dict(test_input, namespace, set_name, key, client):
    await client.put_key(namespace, set_name, key, {"test_bin": test_input})
    result = await client.get_key(namespace, set_name, key)
    assert result["test_bin"] == test_input
