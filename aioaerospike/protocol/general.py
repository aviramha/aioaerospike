from construct import Struct, BytesInteger, Int8ub, Rebuild, Const, Byte, Switch

from .admin import AdminMessage
Int48ub = BytesInteger(6)

a = Struct('version' / Const(2, Int8ub))

MessageType = Enum(Int8ub,
                   INFO=1,
                   ADMIN=2,
                   MESSAGE=3,
                   COMPRESSED=4)

AerospikeMessage = Struct('version' / Const(2, Int8ub),
                          'message_type' / MessageType,
                          'length' / Rebuild(Int48ub,
                                             lambda this:
                                             this._subcons.payload.sizeof()),
                          'data' / Switch(this.message_type,
                                          {
                                              'INFO': Struct(),
                                              'ADMIN', Struct(),
                                              'MESSAGE', Struct(),
                                              'COMPRESSED', Struct()
                                          }),
                          )

#AerospikeMessageHeader = Struct('size', )
6
