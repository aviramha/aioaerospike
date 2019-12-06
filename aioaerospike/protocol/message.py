import hashlib
from dataclasses import dataclass
from enum import IntEnum, IntFlag, auto
from functools import reduce
from struct import Struct
from typing import List, Type, Union

# Can read about the flag in as_command.h (C client)


class Info1Flags(IntFlag):
    empty = 0
    read = auto()
    get_all = auto()
    unused = auto()
    batch_index = auto()
    xdr = auto()
    dont_get_bin_data = auto()
    read_mode_ap_all = auto()
    # Last bit unused


class Info2Flags(IntFlag):
    empty = 0
    write = auto()
    delete = auto()
    generation = auto()
    # apply write if new generation >= old, good for restore
    generation_gt = auto()
    durable_delete = auto()
    create_only = auto()
    unused = auto()
    respond_all_ops = auto()


class Info3Flags(IntFlag):
    empty = 0
    last = auto()
    commit_master = auto()
    unused = auto()
    update_only = auto()
    create_or_replace = auto()
    replace_only = auto()
    sc_read_type = auto()
    sc_read_relax = auto()


class FieldTypes(IntEnum):
    namespace = 0
    setname = 1
    key = 2
    digest = 4
    task_id = 7
    scan_options = 8
    scan_timeout = 9
    scan_rps = 10
    index_range = 22
    index_filter = 23
    index_limit = 24
    index_order = 25
    index_type = 26
    udf_package_name = 30
    udf_function = 31
    udf_arglist = 32
    udf_op = 33
    query_bins = 40
    batch_index = 41
    batch_index_with_set = 42
    predexp = 43


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
    read = 1
    write = 2
    cdt_read = 3
    cdt_modify = 4
    map_read = 6
    map_modify = 7
    incr = 5
    append = 9
    prepend = 10
    touch = 11
    bit_read = 12
    bit_modify = 13
    delete = 14


class BinType(IntEnum):
    undef = 0
    integer = 1
    double = 2
    string = 3
    blob = 4
    java = 7
    csharp = 8
    python = 9
    ruby = 10
    php = 11
    erlang = 12
    tmap = 19
    tlist = 20
    ldt = 21
    geojson = 23


@dataclass
class BinData:
    data: Union[str, bytes, int, float]
    INTEGER_FORMAT = Struct("!Q")
    DOUBLE_FORMAT = Struct("!d")

    def pack(self) -> bytes:
        if isinstance(self.data, str):
            return self.data.encode("utf-8")
        elif isinstance(self.data, bytes):
            return self.data
        elif isinstance(self.data, int):
            return self.INTEGER_FORMAT.pack(self.data)
        elif isinstance(self.data, float):
            return self.DOUBLE_FORMAT.pack(self.data)
        raise TypeError(
            f"Expected [str, bytes, int, float], received {type(self.data)}"
        )

    @classmethod
    def parse(
        cls: Type["BinData"], data_type: BinType, data: bytes
    ) -> "BinData":
        if data_type == BinType.string:
            return cls(data=data.decode("utf-8"))
        elif data_type == BinType.blob:
            return cls(data=data)
        elif data_type == BinType.integer:
            return cls(data=cls.INTEGER_FORMAT.unpack(data)[0])
        elif data_type == BinType.double:
            return cls(data=cls.DOUBLE_FORMAT.unpack(data)[0])
        raise TypeError(f"Expected BinType value, received {data_type}")

    def __len__(self) -> int:
        if isinstance(self.data, str) or isinstance(self.data, bytes):
            return len(self.data)
        # Equals to integer format size.
        return self.DOUBLE_FORMAT.size


@dataclass
class Bin:
    FORMAT = Struct("BBB")
    btype: BinType
    version: int
    name: str
    data: BinData

    def pack(self) -> bytes:
        base = self.FORMAT.pack(self.btype, self.version, len(self.name))
        return base + self.name.encode("utf-8") + self.data.pack()

    @classmethod
    def parse(cls: Type["Bin"], data: bytes) -> "Bin":
        unpacked = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        btype, version, name_length = unpacked
        name = data[cls.FORMAT.size : cls.FORMAT.size + name_length].decode(
            "utf-8"
        )
        data = data[cls.FORMAT.size + name_length :]
        bin_data = BinData.parse(btype, data)
        return cls(btype=btype, name=name, version=version, data=bin_data,)

    def __len__(self):
        return self.FORMAT.size + len(self.name) + len(self.data)


@dataclass
class Operation:
    # Size, Op, Bin data type, Bin version, Bin name length
    FORMAT = Struct("!IB")
    operation_type: OperationTypes
    data_bin: Bin

    def pack(self) -> bytes:
        length = len(self.data_bin) + 1
        return (
            self.FORMAT.pack(length, self.operation_type) + self.data_bin.pack()
        )

    @classmethod
    def parse(cls: Type["Operation"], data: bytes) -> "Operation":
        unpacked = cls.FORMAT.unpack(data[: cls.FORMAT.size])
        size, operation_type = unpacked
        data_bin = Bin.parse(data[cls.FORMAT.size : cls.FORMAT.size + size])
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
            data_left = data[len(op) :]

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


def digest_key(set_name: bytes, key: str) -> bytes:
    ripe = hashlib.new("ripemd160")
    ripe.update(set_name)
    ripe.update(key.encode("utf-8"))
    return ripe.digest()


def put_key(
    namespace: str, set_name: str, key: str, bin_name: str, value: str
) -> Message:
    set_encoded = set_name.encode("utf-8")
    namespace_field = Field(FieldTypes.namespace, namespace.encode("utf-8"))
    set_field = Field(FieldTypes.setname, set_encoded)

    ripe_digest = digest_key(set_encoded, key)
    key_field = Field(FieldTypes.digest, ripe_digest)

    dbin = BinData(value)
    bin_container = Bin(BinType.string, 0, bin_name, dbin)
    op = Operation(OperationTypes.write, bin_container)
    return Message(
        info1=Info1Flags.empty,
        info2=Info2Flags.write,
        info3=Info3Flags.empty,
        transaction_ttl=1000,
        fields=[namespace_field, set_field, key_field],
        operations=[op],
    )


def get_key(namespace: str, set_name: str, key: str) -> Message:
    set_encoded = set_name.encode("utf-8")
    namespace_field = Field(FieldTypes.namespace, namespace.encode("utf-8"))
    set_field = Field(FieldTypes.setname, set_encoded)

    ripe_digest = digest_key(set_encoded, key)
    key_field = Field(FieldTypes.digest, ripe_digest)

    return Message(
        info1=Info1Flags.read | Info1Flags.get_all,
        info2=Info2Flags.empty,
        info3=Info3Flags.empty,
        transaction_ttl=1000,
        fields=[namespace_field, set_field, key_field],
        operations=[],
    )
