# Changelog for aioaerospike

## 0.1.6 (XXXX-XX-XX)

## 0.1.5 (2019-12-17)
- Added TTL argument for put_key
- Added operate method, enables users to interact with lower-level API to do specific actions, such as multi op
  (read, write, modify, etc) in same message.
- Added UNDEF/AerospikeNone for the option of empty bins, when reading specific bins.

## 0.1.4 (2019-12-07)
- Added delete key method
- Added key_exists method
- Changed signature of put_key to be a dict, for easy multiple bins insert.

## 0.1.3 (2019-12-07) (Unreleased)
- Changed all enums to uppercase
- Added tests for all supported key types
- Added support for dict and list as values.

## 0.1.2 (2019-12-07)
- Fixed key digest, key type can be all supported types (int, float, str, bytes)

## 0.1.1 (2019-12-07)
- Fixed license and metadata

## 0.1.0 (2019-12-07)

- Initial release.
