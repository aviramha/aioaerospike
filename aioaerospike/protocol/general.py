from .admin import AdminMessage

from construct import Struct, BytesInteger, Int8ub, Container, Const
from typing import Union
from dataclasses import dataclass
from enum import IntEnum
from bcrypt import hashpw


class MessageType(IntEnum):
    info = 1
    admin = 2
    message = 3
    compressed = 4


MESSAGE_TYPE_TO_CLASS = {
    MessageType.admin: AdminMessage
}

MESSAGE_CLASS_TO_TYPE = {
    AdminMessage: MessageType.admin
}


@dataclass
class AerospikeHeader:
    # Using construct because it's easier for 48bit integer.
    FORMAT = Struct('version' / Const(2, Int8ub),
                    'message_type' / Int8ub,
                    'length' / BytesInteger(6)
                    )
    message_type: MessageType
    length: int

    def pack(self) -> bytes:
        return self.FORMAT.build(
            Container(message_type=self.message_type, length=self.length)
        )

    @classmethod
    def parse(cls: 'AerospikeHeader', data: bytes) -> 'AerospikeHeader':
        parsed_data = cls.FORMAT.parse(data)
        return cls(message_type=parsed_data.message_type,
                   length=parsed_data.length)


@dataclass
class AerospikeMessage:

    message: Union[AdminMessage]

    def pack(self) -> bytes:
        packed_message = self.message.pack()
        packed_header = AerospikeHeader(
            MESSAGE_CLASS_TO_TYPE[type(self.message)], len(packed_message))
        return packed_header.pack() + packed_message

    @classmethod
    def parse(cls: 'AerospikeMessage', data: bytes) -> 'AerospikeMessage':
        header = AerospikeHeader.parse(data[:AerospikeHeader.FORMAT.sizeof()])
        message_class = MESSAGE_TYPE_TO_CLASS[header.message_type]
        message = message_class.parse(data[AerospikeHeader.FORMAT.sizeof():])
        return cls(message=message)

