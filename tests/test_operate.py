import pytest

from aioaerospike.protocol.message import (
    Bin,
    Info1Flags,
    Info2Flags,
    Info3Flags,
    Operation,
    OperationTypes,
)


@pytest.mark.asyncio
async def test_simple(namespace, set_name, client):
    operations = [
        Operation(OperationTypes.WRITE, Bin.create("test_bin", 123123)),
        Operation(OperationTypes.WRITE, Bin.create("test_bin2", "test_value")),
    ]
    msg = await client.operate(
        namespace,
        set_name,
        "test_key",
        Info1Flags.EMPTY,
        Info2Flags.WRITE,
        Info3Flags.EMPTY,
        operations,
    )
    assert msg.message.result_code == 0

    operations = [
        Operation(OperationTypes.WRITE, Bin.create("test_bin", 9999)),
        Operation(OperationTypes.READ, Bin.create("test_bin2", None)),
        Operation(OperationTypes.READ, Bin.create("test_bin", None)),
    ]
    msg = await client.operate(
        namespace,
        set_name,
        "test_key",
        Info1Flags.READ,
        Info2Flags.WRITE,
        Info3Flags.EMPTY,
        operations,
    )
    assert msg.message.result_code == 0
    data = {
        op.data_bin.name: op.data_bin.data.value
        for op in msg.message.operations
    }
    assert data["test_bin"] == 9999
    assert data["test_bin2"] == "test_value"
