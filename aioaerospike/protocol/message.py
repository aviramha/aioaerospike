from dataclasses import dataclass
from enum import IntEnum, IntFlag, auto
from functools import reduce
from struct import Struct
from typing import Any, Dict, List, Optional, Type

from .datatypes import (
    AerospikeDataType,
    AerospikeKeyType,
    AerospikeValueType,
    data_to_aerospike_type,
    parse_raw,
)

# Can read about the flag in as_command.h (C client)


class Info1Flags(IntFlag):
    EMPTY = 0
    READ = auto()
    GET_ALL = auto()
    UNUSED = auto()
    BATCH_INDEX = auto()
    XDR = auto()
    DONT_GET_BIN_DATA = auto()
    READ_MODE_AP_ALL = auto()
    # Last bit unused


class Info2Flags(IntFlag):
    EMPTY = 0
    WRITE = auto()
    DELETE = auto()
    GENERATION = auto()
    # apply write if new generation >= old, good for RESTORE
    GENERATION_GT = auto()
    DURABLE_DELETE = auto()
    CREATE_ONLY = auto()
    UNUSED = auto()
    RESPOND_ALL_OPS = auto()


class Info3Flags(IntFlag):
    EMPTY = 0
    LAST = auto()
    COMMIT_MASTER = auto()
    UNUSED = auto()
    UPDATE_ONLY = auto()
    CREATE_OR_REPLACE = auto()
    REPLACE_ONLY = auto()
    SC_READ_TYPE = auto()
    SC_READ_RELAX = auto()


class FieldTypes(IntEnum):
    NAMESPACE = 0
    SETNAME = 1
    KEY = 2
    DIGEST = 4
    TASK_ID = 7
    SCAN_OPTIONS = 8
    SCAN_TIMEOUT = 9
    SCAN_RPS = 10
    INDEX_RANGE = 22
    INDEX_FILTER = 23
    INDEX_LIMIT = 24
    INDEX_ORDER = 25
    INDEX_TYPE = 26
    UDF_PACKAGE_NAME = 30
    UDF_FUNCTION = 31
    UDF_ARGLIST = 32
    UDF_OP = 33
    QUERY_BINS = 40
    BATCH_INDEX = 41
    BATCH_INDEX_WITH_SET = 42
    PREDEXP = 43


@dataclass
class Field:
    FORMAT = Struct("!IB")
    field_type: FieldTypes
    data: bytes

    def pack(self) -> bytes:
        length = len(self.data) + 1
        return self.FORMAT.pack(length, self.field_type) + self.data

    @classmethod
    def parse(cls: Type["Field"], data: bytes) -> "Field":
        length, field_type = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        data = data[cls.FORMAT.size : length]
        return cls(field_type=field_type, data=data)

    def __len__(self):
        return len(self.data) + self.FORMAT.size


class OperationTypes(IntEnum):
    READ = 1
    WRITE = 2
    CDT_READ = 3
    CDT_MODIFY = 4
    MAP_READ = 6
    MAP_MODIFY = 7
    INCR = 5
    APPEND = 9
    PREPEND = 10
    TOUCH = 11
    BIT_READ = 12
    BIT_MODIFY = 13
    DELETE = 14


@dataclass
class Bin:
    FORMAT = Struct("BBB")
    version: int
    name: str
    data: AerospikeDataType

    def pack(self) -> bytes:
        base = self.FORMAT.pack(self.data.TYPE, self.version, len(self.name))
        return base + self.name.encode("utf-8") + self.data.pack()

    @classmethod
    def parse(cls: Type["Bin"], data: bytes) -> "Bin":
        unpacked = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        btype, version, name_length = unpacked
        name = data[cls.FORMAT.size : cls.FORMAT.size + name_length].decode(
            "utf-8"
        )
        data = data[cls.FORMAT.size + name_length :]
        bin_data = parse_raw(btype, data)
        return cls(name=name, version=version, data=bin_data)

    def __len__(self):
        return self.FORMAT.size + len(self.name) + len(self.data)

    @classmethod
    def create(cls, name: str, data: Any, version=0) -> "Bin":
        adata = data_to_aerospike_type(data)
        return cls(name=name, version=version, data=adata)


@dataclass
class Operation:
    # Size, Op, Bin data type, Bin version, Bin name length
    FORMAT = Struct("!IB")
    operation_type: OperationTypes
    data_bin: Bin

    def pack(self) -> bytes:
        packed_bin = self.data_bin.pack()
        length = len(packed_bin) + 1
        return self.FORMAT.pack(length, self.operation_type) + packed_bin

    @classmethod
    def parse(cls: Type["Operation"], data: bytes) -> "Operation":
        unpacked = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        size, operation_type = unpacked
        data_bin = Bin.parse(data[cls.FORMAT.size : cls.FORMAT.size + size - 1])
        return cls(operation_type=operation_type, data_bin=data_bin)

    def __len__(self):
        return len(self.data_bin) + self.FORMAT.size


@dataclass
class Message:
    FORMAT = Struct("!BBBBxBIIIHH")
    info1: Info1Flags
    info2: Info2Flags
    info3: Info3Flags
    transaction_ttl: int
    fields: List[Field]
    operations: List[Operation]
    result_code: int = 0
    generation: int = 0
    record_ttl: int = 0

    def pack(self) -> bytes:
        base = self.FORMAT.pack(
            self.FORMAT.size,
            self.info1,
            self.info2,
            self.info3,
            self.result_code,
            self.generation,
            self.record_ttl,
            self.transaction_ttl,
            len(self.fields),
            len(self.operations),
        )
        fields = reduce(
            lambda x, y: x + y, (field.pack() for field in self.fields), b""
        )
        operations = reduce(
            lambda x, y: x + y, (op.pack() for op in self.operations), b""
        )
        return base + fields + operations

    @classmethod
    def parse(cls: Type["Message"], data: bytes) -> "Message":
        parsed_tuple = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        (
            _size,
            info1,
            info2,
            info3,
            result_code,
            generation,
            ttl,
            transaction_ttl,
            fields_count,
            operations_count,
        ) = parsed_tuple
        data_left = data[cls.FORMAT.size :]
        fields = []
        operations = []
        for _i in range(0, fields_count):
            f = Field.parse(data_left)
            fields.append(f)
            data_left = data[len(f) :]

        for _i in range(0, operations_count):
            op = Operation.parse(data_left)
            operations.append(op)
            data_left = data_left[len(op) :]

        return cls(
            info1=info1,
            info2=info2,
            info3=info3,
            result_code=result_code,
            generation=generation,
            record_ttl=ttl,
            transaction_ttl=transaction_ttl,
            fields=fields,
            operations=operations,
        )


def generate_namespace_set_key_fields(
    namespace: str, set_name: str, key: AerospikeKeyType
) -> List[Field]:
    set_encoded = set_name.encode("utf-8")
    namespace_field = Field(FieldTypes.NAMESPACE, namespace.encode("utf-8"))
    set_field = Field(FieldTypes.SETNAME, set_encoded)

    aero_key = data_to_aerospike_type(key)
    key_field = Field(FieldTypes.DIGEST, aero_key.digest(set_name))

    return [namespace_field, set_field, key_field]


def put_key(
    namespace: str,
    set_name: str,
    key: AerospikeKeyType,
    bin_: Dict[str, AerospikeValueType],
    ttl: int = 0,
) -> Message:
    fields = generate_namespace_set_key_fields(namespace, set_name, key)

    ops = []
    for k, v in bin_.items():
        op = Operation(OperationTypes.WRITE, Bin.create(name=k, data=v))
        ops.append(op)

    return Message(
        info1=Info1Flags.EMPTY,
        info2=Info2Flags.WRITE,
        info3=Info3Flags.EMPTY,
        transaction_ttl=1000,
        fields=fields,
        operations=ops,
        record_ttl=ttl,
    )


def get_key(namespace: str, set_name: str, key: AerospikeKeyType) -> Message:
    fields = generate_namespace_set_key_fields(namespace, set_name, key)

    return Message(
        info1=Info1Flags.READ | Info1Flags.GET_ALL,
        info2=Info2Flags.EMPTY,
        info3=Info3Flags.EMPTY,
        transaction_ttl=1000,
        fields=fields,
        operations=[],
    )


def delete_key(namespace: str, set_name: str, key: AerospikeKeyType) -> Message:
    fields = generate_namespace_set_key_fields(namespace, set_name, key)

    return Message(
        info1=Info1Flags.EMPTY,
        info2=Info2Flags.DELETE | Info2Flags.WRITE,
        info3=Info3Flags.EMPTY,
        transaction_ttl=1000,
        fields=fields,
        operations=[],
    )


def key_exists(namespace: str, set_name: str, key: AerospikeKeyType) -> Message:
    fields = generate_namespace_set_key_fields(namespace, set_name, key)

    return Message(
        info1=Info1Flags.READ | Info1Flags.DONT_GET_BIN_DATA,
        info2=Info2Flags.EMPTY,
        info3=Info3Flags.EMPTY,
        transaction_ttl=1000,
        fields=fields,
        operations=[],
    )


def operate(
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
):
    fields = fields or []
    fields += generate_namespace_set_key_fields(namespace, set_name, key)
    return Message(
        info1=info1,
        info2=info2,
        info3=info3,
        transaction_ttl=1000,
        fields=fields,
        operations=operations,
        generation=generation,
        record_ttl=ttl,
    )
