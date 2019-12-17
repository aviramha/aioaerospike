from asyncio import StreamReader, StreamWriter, open_connection
from functools import wraps
from typing import Any, Dict, List, Optional

from .protocol.datatypes import AerospikeKeyType, AerospikeValueType
from .protocol.general import AerospikeHeader, AerospikeMessage
from .protocol.message import (
    Field,
    Info1Flags,
    Info2Flags,
    Info3Flags,
    Operation,
    delete_key,
    get_key,
    key_exists,
    operate,
    put_key,
)


class AerospikeClientNotConnected(Exception):
    pass


def require_connection(func):
    @wraps(func)
    async def wrapper(
        self: "AerospikeClient", *args: List[Any], **kwargs: Dict[Any, Any]
    ) -> Any:
        if not self._reader or not self._writer:
            raise AerospikeClientNotConnected()
        return await func(self, *args, **kwargs)

    return wrapper


class AerospikeClient:

    __slots__ = [
        "host",
        "port",
        "_user",
        "_password",
        "_use_ssl",
        "_reader",
        "_writer",
    ]

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        use_ssl: bool = False,
        port: int = 3000,
    ):
        self.host: str = host
        self.port: int = port
        self._user: str = user
        self._password: str = password
        self._use_ssl: bool = use_ssl
        self._reader: Optional[StreamReader] = None
        self._writer: Optional[StreamWriter] = None

    async def connect(self):
        self._reader, self._writer = await open_connection(self.host, self.port)

    @require_connection
    async def _get_response(self) -> AerospikeMessage:
        header_data = await self._reader.readexactly(
            AerospikeHeader.FORMAT.sizeof()
        )
        header = AerospikeHeader.parse(header_data)
        message_data = await self._reader.readexactly(header.length)
        message = AerospikeMessage.parse(header_data + message_data)
        return message

    @require_connection
    async def put_key(
        self,
        namespace: str,
        set_name: str,
        key: str,
        bin_: Dict[str, AerospikeValueType],
        ttl: int = 0,
    ) -> None:
        message = put_key(namespace, set_name, key, bin_)
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        response = await self._get_response()
        if response.message.result_code != 0:
            raise Exception(
                f"Unexpected result code {response.message.result_code}"
            )

    @require_connection
    async def get_key(self, namespace: str, set_name: str, key: str) -> Any:
        message = get_key(namespace, set_name, key)
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        response = await self._get_response()
        return {
            op.data_bin.name: op.data_bin.data.value
            for op in response.message.operations
        }

    @require_connection
    async def delete_key(self, namespace: str, set_name: str, key: str) -> None:
        message = delete_key(namespace, set_name, key)
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        response = await self._get_response()
        if response.message.result_code != 0:
            raise Exception(
                f"Unexpected result code {response.message.result_code}"
            )

    @require_connection
    async def key_exists(self, namespace: str, set_name: str, key: str) -> bool:
        message = key_exists(namespace, set_name, key)
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        response = await self._get_response()
        if response.message.result_code == 2:
            return False
        elif response.message.result_code != 0:
            raise Exception(
                f"Unexpected result code {response.message.result_code}"
            )
        return True

    @require_connection
    async def operate(
        self,
        namespace: str,
        set_name: str,
        key: AerospikeKeyType,
        info1: Info1Flags,
        info2: Info2Flags,
        info3: Info3Flags,
        operations: List[Operation],
        fields: Optional[List[Field]] = None,
        ttl: int = 0,
        generation: int = 0,
    ) -> AerospikeMessage:
        """
        Execute the given operations, letting user define their own custom complex operations.
        Fields will be appended to the default fields of namespace, set, key.
        """
        message = operate(
            namespace,
            set_name,
            key,
            info1,
            info2,
            info3,
            operations,
            fields,
            ttl,
            generation,
        )
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        return await self._get_response()
