from dataclasses import dataclass
from enum import IntEnum
from struct import Struct
from typing import List, Type

from bcrypt import hashpw

BCRYPT_SALT = b"$2a$10$7EqJtq98hPqEX7fNZaFWoO"


class AdminCommandsType(IntEnum):
    authenticate = 0
    create_user = 1
    drop_user = 2
    set_password = 3
    change_password = 4
    grant_roles = 5
    revoke_roles = 6
    query_users = 9
    create_role = 10
    drop_role = 11
    grant_privileges = 12
    revoke_privileges = 13
    set_whitelist = 14
    query_roles = 16
    login = 20


class FieldTypes(IntEnum):
    user = 0
    password = 1
    old_password = 2
    credential = 3
    clear_password = 4
    session_token = 5
    session_ttl = 6
    roles = 10
    role = 11
    privileges = 12
    whitelist = 13


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
        user_field = Field(FieldTypes.user, user.encode("utf-8"))
        password_field = Field(FieldTypes.password, hashed_pass)
        return cls(
            command_type=AdminCommandsType.login,
            fields=[user_field, password_field],
        ).pack()


def hash_password(password: str) -> bytes:
    """
    Hashes password according to Aerospike algorithm
    """
    return hashpw(password.encode("utf-8"), BCRYPT_SALT)
