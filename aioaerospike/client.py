from asyncio import open_connection, StreamReader, StreamWriter

from .protocol.general import AerospikeMessage, AerospikeHeader
from .protocol.admin import AdminMessage


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
        self._reader: StreamReader = None
        self._writer: StreamWriter = None

    async def connect(self):
        self._reader, self._writer = await open_connection(self.host, self.port)
        await self._login()

    async def _login(self):
        login_message = AdminMessage.login(self._user, self._password)
        aerospike_message = AerospikeMessage(login_message)
        self._writer.write(aerospike_message.pack())
        await self._writer.drain()
        header = await self._reader.readexactly(AerospikeHeader.FORMAT.sizeof())
        parsed_header = AerospikeHeader.parse(header)
        rest_of_data = await self._reader.readexactly(parsed_header.length)
        message = AerospikeMessage.parse(header + rest_of_data)