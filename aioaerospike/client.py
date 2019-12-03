from asyncio import StreamReader, StreamWriter, open_connection
from typing import Optional

# from .protocol.general import AerospikeMessage, AerospikeHeader
# from .protocol.admin import AdminMessage


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
