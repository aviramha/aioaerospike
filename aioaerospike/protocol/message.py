from dataclasses import dataclass
from struct import Struct, unpack

from typing import Optional, List, Union

from enum import IntFlag, IntEnum, auto

# Can read about the flag in as_command.h (C client)


class Info1Flags(IntFlag):
    read = auto()
    get_all = auto()
    unused = auto()
    batch_index = auto()
    xdr = auto()
    dont_get_bin_data = auto()
    read_mode_ap_all = auto()
    # Last bit unused


class Info2Flags(IntFlag):
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
        length = len(self.data)
        return self.FORMAT.pack(length, self.field_type) + self.data

    @classmethod
    def parse(cls: 'Field', data: bytes) -> 'Field':
        length, field_type = cls.FORMAT.unpack(data[:cls.FORMAT.size])
        data = data[cls.FORMAT.size:length]
        return cls(field_type=field_type, data=data)

    def __len__(self):
        return len(self.data)


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


def data_bin_to_python(
    data: bytes,
    dtype: BinType
) -> Union[str, bytes, int, float]:
    if dtype == BinType.string:
        # Consider changing this later to have optional encoding.
        return data.decode('utf-8')
    elif dtype == BinType.integer:
        return unpack('!Q', data)[0]
    elif dtype == BinType.double:
        return unpack("!d", data)[0]
    return data


@dataclass
class Operation:
    # Size, Op, Bin data type, Bin version, Bin name length
    FORMAT = Struct("!IBBBB")
    operation_type: OperationTypes
    bin_type: BinType
    bin_version: int
    bin_name: str
    data: Union[str, bytes, int, float]

    def pack(self) -> bytes:
        length = len(self.data)
        return self.FORMAT.pack(length, self.field_type) + self.data

    @classmethod
    def parse(cls: 'Operation', data: bytes) -> 'Operation':
        unpacked = cls.FORMAT.unpack(data[:cls.FORMAT.size])
        size, op_type, bin_type, bin_version, bin_name_length = unpacked
        bin_name = data[cls.FORMAT.size:bin_name_length]
        data = data[cls.FORMAT.size+bin_name_length:]
        return cls(field_type=field_type, data=data)

    def __len__(self):
        return len(self.data)


@dataclass
class MessageHeader:
    FORMAT = '!BBBx'
    info1: Info1Flags
    info2: Info2Flags
    info3: Info3Flags
    result_code: int
    generation: int
    ttl: int
    transaction_ttl: int
    fields: List[Field]
    operations: List[Operation]
