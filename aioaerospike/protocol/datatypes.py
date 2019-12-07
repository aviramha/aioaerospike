import hashlib
from abc import ABCMeta, abstractmethod
from enum import IntEnum
from struct import Struct, pack
from typing import Any, ClassVar, Dict, Optional, Type, Union

AerospikeKeyType = Union[str, bytes, float, int]


class AerospikeTypes(IntEnum):
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


PYTHON_TYPE_TO_AEROSPIKE = {
    str: AerospikeTypes.string,
    bytes: AerospikeTypes.blob,
    float: AerospikeTypes.double,
    int: AerospikeTypes.integer,
}


class AerospikeMetaDataType(ABCMeta):
    types: ClassVar[Dict[AerospikeTypes, Type["AerospikeDataType"]]] = {}

    def __new__(cls, *args: Any, **kwargs: Any):
        new_class = super().__new__(cls, *args, **kwargs)
        type_ = getattr(new_class, "TYPE", None)
        if type_:
            cls.types[type_] = new_class
        return new_class


class AerospikeDataType(metaclass=AerospikeMetaDataType):

    DIGESTABLE = False
    TYPE: Optional[AerospikeTypes] = None

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
        ripe.update(pack("!B", self.TYPE.value) + self.pack())
        return ripe.digest()


class AerospikeInteger(AerospikeDataType):
    TYPE = AerospikeTypes.integer
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
    TYPE = AerospikeTypes.double
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
    TYPE = AerospikeTypes.string
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
    TYPE = AerospikeTypes.blob
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


def parse_raw(atype: AerospikeTypes, data: bytes) -> AerospikeDataType:
    return AerospikeMetaDataType.types[atype].parse(data)


def data_to_aerospike_type(data: Any) -> AerospikeDataType:
    aerotype = PYTHON_TYPE_TO_AEROSPIKE[type(data)]
    return AerospikeMetaDataType.types[aerotype](data)
