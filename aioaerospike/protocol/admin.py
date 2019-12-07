from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import List, Type

from bcrypt import hashpw

BCRYPT_SALT = b"$2a$10$7EqJtq98hPqEX7fNZaFWoO"


class AdminCommandsType(IntEnum):
    AUTHENTICATE = 0
    CREATE_USER = 1
    DROP_USER = 2
    SET_PASSWORD = 3
    CHANGE_PASSWORD = 4
    GRANT_ROLES = 5
    REVOKE_ROLES = 6
    QUERY_USERS = 9
    CREATE_ROLE = 10
    DROP_ROLE = 11
    GRANT_PRIVILEGES = 12
    REVOKE_PRIVILEGES = 13
    SET_WHITELIST = 14
    QUERY_ROLES = 16
    LOGIN = 20


class FieldTypes(IntEnum):
    USER = 0
    PASSWORD = 1
    OLD_PASSWORD = 2
    CREDENTIAL = 3
    CLEAR_PASSWORD = 4
    SESSION_TOKEN = 5
    SESSION_TTL = 6
    ROLES = 10
    ROLE = 11
    PRIVILEGES = 12
    WHITELIST = 13


@dataclass
class Field:
    FORMAT = Struct("!IB")
    field_type: FieldTypes
    data: bytes

    def pack(self) -> bytes:
        length = len(self.data)
        return self.FORMAT.pack(length, self.field_type) + self.data

    @classmethod
    def parse(cls: Type["Field"], data: bytes) -> "Field":
        length, field_type = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        data = data[cls.FORMAT.size : length]
        return cls(field_type=field_type, data=data)

    def __len__(self):
        return len(self.data)


@dataclass
class AdminMessage:
    FORMAT = Struct("!16xBB")
    command_type: AdminCommandsType
    fields: List[Field]

    def pack(self) -> bytes:
        fields_count = len(self.fields)
        fields_data = b""
        for field in self.fields:
            fields_data += field.pack()
        return self.FORMAT.pack(self.command_type, fields_count) + fields_data

    @classmethod
    def parse(cls: Type["AdminMessage"], data: bytes) -> "AdminMessage":
        command_type, fields_count = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        fields = []
        data_left = data[cls.FORMAT.size :]
        for _i in range(fields_count):
            field = Field.parse(data_left)
            fields.append(field)
            data_left = data_left[Field.FORMAT.size + len(field) :]
        return cls(fields=fields, command_type=command_type)

    @classmethod
    def login(cls: Type["AdminMessage"], user: str, password: str) -> bytes:
        hashed_pass = hash_password(password)
        user_field = Field(FieldTypes.USER, user.encode("utf-8"))
        password_field = Field(FieldTypes.PASSWORD, hashed_pass)
        return cls(
            command_type=AdminCommandsType.LOGIN,
            fields=[user_field, password_field],
        ).pack()


def hash_password(password: str) -> bytes:
    """
    Hashes password according to Aerospike algorithm
    """
    return hashpw(password.encode("utf-8"), BCRYPT_SALT)
