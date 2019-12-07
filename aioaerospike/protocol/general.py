from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Dict, Type, Union

from construct import BytesInteger, Const, Container, Int8ub, Struct

from .admin import AdminMessage
from .message import Message


class MessageType(IntEnum):
    INFO = 1
    ADMIN = 2
    MESSAGE = 3
    COMPRESSED = 4


MESSAGE_TYPE_TO_CLASS: Dict[MessageType, Type[Any]] = {
    MessageType.ADMIN: AdminMessage,
    MessageType.MESSAGE: Message,
}

MESSAGE_CLASS_TO_TYPE = {
    AdminMessage: MessageType.ADMIN,
    Message: MessageType.MESSAGE,
}


@dataclass
class AerospikeHeader:
    # Using construct because it's easier for 48bit integer.
    FORMAT = Struct(
        "version" / Const(2, Int8ub),
        "message_type" / Int8ub,
        "length" / BytesInteger(6),
    )
    message_type: MessageType
    length: int

    def pack(self) -> bytes:
        return self.FORMAT.build(
            Container(message_type=self.message_type, length=self.length)
        )

    @classmethod
    def parse(cls: Type["AerospikeHeader"], data: bytes) -> "AerospikeHeader":
        parsed_data = cls.FORMAT.parse(data)
        return cls(
            message_type=parsed_data.message_type, length=parsed_data.length
        )


@dataclass
class AerospikeMessage:

    message: Union[Message]

    def pack(self) -> bytes:
        packed_message = self.message.pack()
        packed_header = AerospikeHeader(
            message_type=MESSAGE_CLASS_TO_TYPE[type(self.message)],
            length=len(packed_message),
        )
        return packed_header.pack() + packed_message

    @classmethod
    def parse(cls: Type["AerospikeMessage"], data: bytes) -> "AerospikeMessage":
        header = AerospikeHeader.parse(data[: AerospikeHeader.FORMAT.sizeof()])
        message_class = MESSAGE_TYPE_TO_CLASS[header.message_type]
        message = message_class.parse(data[AerospikeHeader.FORMAT.sizeof() :])
        return cls(message=message)
