import hashlib
import struct
from abc import ABCMeta, abstractmethod
from enum import IntEnum
from struct import Struct
from typing import Any, ClassVar, Dict, List, Optional, Type, Union

import msgpack

AerospikeKeyType = Union[str, bytes, float, int]
AerospikeValueType = Union[str, bytes, float, int, list, dict]


class AerospikeTypes(IntEnum):
    UNDEF = 0
    INTEGER = 1
    DOUBLE = 2
    STRING = 3
    BLOB = 4
    JAVA = 7
    CSHARP = 8
    PYTHON = 9
    RUBY = 10
    PHP = 11
    ERLANG = 12
    TMAP = 19
    TLIST = 20
    LDT = 21
    GEOJSON = 23


PYTHON_TYPE_TO_AEROSPIKE = {
    str: AerospikeTypes.STRING,
    bytes: AerospikeTypes.BLOB,
    float: AerospikeTypes.DOUBLE,
    int: AerospikeTypes.INTEGER,
    list: AerospikeTypes.TLIST,
    dict: AerospikeTypes.TMAP,
    type(None): AerospikeTypes.UNDEF,
}


class AerospikeMetaDataType(ABCMeta):
    types: ClassVar[Dict[AerospikeTypes, Type["AerospikeDataType"]]] = {}

    def __new__(cls, *args: Any, **kwargs: Any):
        new_class = super().__new__(cls, *args, **kwargs)
        type_ = getattr(new_class, "TYPE", None)
        if type_ is not None:
            cls.types[type_] = new_class
        return new_class


class AerospikeDataType(metaclass=AerospikeMetaDataType):

    DIGESTABLE = False
    TYPE: Optional[AerospikeTypes] = None
    value: Any

    @abstractmethod
    def __init__(self, value: Any) -> None:
        pass

    @abstractmethod
    def pack(self) -> bytes:
        """
        Packs the datatype
        """
        pass

    @classmethod
    @abstractmethod
    def parse(cls, data: bytes) -> "AerospikeDataType":
        """
        Parses data and returns class instance.
        """
        pass

    def digest(self, set_name: str) -> bytes:
        """
        Create RIPEMD160 digest from the datatype on supported once
        """
        if not self.DIGESTABLE:
            raise NotImplementedError(
                f"digest not available for class {type(self)}"
            )
        ripe = hashlib.new("ripemd160")
        ripe.update(set_name.encode("utf-8"))
        ripe.update(struct.pack("!B", self.TYPE.value) + self.pack())
        return ripe.digest()


class AerospikeInteger(AerospikeDataType):
    TYPE = AerospikeTypes.INTEGER
    FORMAT = Struct("!Q")
    DIGESTABLE = True

    def __init__(self, value: int):
        self.value = value

    def pack(self) -> bytes:
        return self.FORMAT.pack(self.value)

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeInteger":
        return cls(cls.FORMAT.unpack(data)[0])

    def __len__(self):
        return self.FORMAT.size


class AerospikeDouble(AerospikeDataType):
    TYPE = AerospikeTypes.DOUBLE
    FORMAT = Struct("!d")
    DIGESTABLE = True

    def __init__(self, value: float):
        self.value = value

    def pack(self) -> bytes:
        return self.FORMAT.pack(self.value)

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeDouble":
        return cls(cls.FORMAT.unpack(data)[0])

    def __len__(self):
        return self.FORMAT.size


class AerospikeString(AerospikeDataType):
    TYPE = AerospikeTypes.STRING
    DIGESTABLE = True

    def __init__(self, value: str):
        self.value = value

    def pack(self) -> bytes:
        return self.value.encode("utf-8")

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeString":
        return cls(data.decode("utf-8"))

    def __len__(self):
        return len(self.value)


class AerospikeBytes(AerospikeDataType):
    TYPE = AerospikeTypes.BLOB
    DIGESTABLE = True

    def __init__(self, value: bytes):
        self.value = value

    def pack(self) -> bytes:
        return self.value

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeBytes":
        return cls(data)

    def __len__(self):
        return len(self.value)


class AerospikeNone(AerospikeDataType):
    TYPE = AerospikeTypes.UNDEF

    def __init__(self, value: None):
        pass

    def pack(self) -> bytes:
        return b""

    @classmethod
    def parse(cls, data: bytes) -> None:
        return None

    def __len__(self):
        return 0


class AerospikeList(AerospikeDataType):
    TYPE = AerospikeTypes.TLIST
    DIGESTABLE = False

    def __init__(self, value: List[Any], size=0) -> None:
        self.value = value
        self._size = size

    def pack(self) -> bytes:
        aerospike_list = []
        for val in self.value:
            aerospike_list.append(pack_native(val))
        data = msgpack.packb(aerospike_list)
        self._size = len(data)
        return data

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeList":
        raw_values = msgpack.unpackb(data)
        parsed_values = []
        for value in raw_values:
            parsed_values.append(unpack_aerospike(value))
        return cls(parsed_values, len(data))

    def __len__(self) -> int:
        if self._size:
            return self._size
        return len(self.pack())


class AerospikeMap(AerospikeDataType):
    TYPE = AerospikeTypes.TMAP
    DIGESTABLE = False

    def __init__(self, value: Dict[Any, Any], size=0) -> None:
        self.value = value
        self._size = size

    def pack(self) -> bytes:
        aerospike_dict = {}
        for k, v in self.value.items():
            packed_k = pack_native(k)
            packed_v = pack_native(v)
            aerospike_dict[packed_k] = packed_v
        data = msgpack.packb(aerospike_dict)
        self._size = len(data)
        return data

    @classmethod
    def parse(cls, data: bytes) -> "AerospikeMap":
        raw_dict = msgpack.unpackb(data)
        parsed_dict = {}
        for k, v in raw_dict.items():
            parsed_dict[unpack_aerospike(k)] = unpack_aerospike(v)
        return cls(parsed_dict, len(data))

    def __len__(self) -> int:
        if self._size:
            return self._size
        return len(self.pack())


def parse_raw(atype: AerospikeTypes, data: bytes) -> AerospikeDataType:
    return AerospikeMetaDataType.types[atype].parse(data)


def data_to_aerospike_type(data: Any) -> AerospikeDataType:
    aerotype = PYTHON_TYPE_TO_AEROSPIKE[type(data)]
    return AerospikeMetaDataType.types[aerotype](data)


def pack_native(data: Any) -> bytes:
    """
    Packs a native python object for list, arrays and dicts
    """
    type_instance = data_to_aerospike_type(data)
    return struct.pack("!B", type_instance.TYPE) + type_instance.pack()


def unpack_aerospike(data: bytes) -> AerospikeValueType:
    """
    Unpacks data that was part of a list,dict,array in Aerospike format
    returns native type
    """
    atype = AerospikeTypes(data[0])
    parsed = parse_raw(atype, data[1:])
    return parsed.value
