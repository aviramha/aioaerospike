from asyncio import StreamReader, StreamWriter, open_connection
from typing import Optional

from .protocol.general import AerospikeMessage, AerospikeHeader
from .protocol.message import put_key


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

    async def _get_response(self) -> AerospikeMessage:
        header_data = await self._reader.readexactly(AerospikeHeader.FORMAT.sizeof())
        header = AerospikeHeader.parse(header_data)
        message_data = await self._reader.readexactly(header.length)
        message = AerospikeMessage.parse(header_data + message_data)
        return message

    async def put_key(self, namespace: str, set_name: str, key: str, bin_name: str, value: str) -> None:
        message = put_key(namespace, set_name, key, bin_name, value)
        data = AerospikeMessage(message).pack()
        self._writer.write(data)
        await self._writer.drain()
        response = await self._get_response()
        if response.message.result_code != 0:
            raise Exception(f"Unexpected result code {response.message.result_code}")
