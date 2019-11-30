from construct import (Struct,
                       Const,
                       Enum,
                       Int8ub,
                       Int32ub,
                       Array,
                       Rebuild,
                       len_,
                       this,
                       Byte,
                       Bytes)

CommandTypes = Enum(Int8ub,
                    AUTHENTICATE=0,
                    CREATE_USER=1,
                    DROP_USER=2,
                    SET_PASSWORD=3,
                    CHANGE_PASSWORD=4,
                    GRANT_ROLES=5,
                    REVOKE_ROLES=6,
                    QUERY_USERS=9,
                    CREATE_ROLE=10,
                    DROP_ROLE=11,
                    GRANT_PRIVILEGES=12,
                    REVOKE_PRIVILEGES=13,
                    SET_WHITELIST=14,
                    QUERY_ROLES=16,
                    LOGIN=20,
                    )

FieldIDs = Enum(Int8ub,
                USER=0,
                PASSWORD=1,
                OLD_PASSWORD=2,
                CREDENTIAL=3,
                CLEAR_PASSWORD=4,
                SESSION_TOKEN=5,
                SESSION_TTL=6,
                ROLES=10,
                ROLE=11,
                PRIVILEGES=12,
                WHITELIST=13,)

Field = Struct('length' / Rebuild(Int32ub, len_(this.data)),
               'field_id' / FieldIDs,
               'data' / Bytes(this.length)
               )

AdminMessage = Struct('padding' / Const(b'\x00' * 16),
                      'command' / CommandTypes,
                      'fields_count' /
                      Rebuild(Int8ub, len_(this.fields)),
                      'fields' / Array(this.fields_count, Field)
                      )
