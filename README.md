# aioaerospike
[![codecov](https://codecov.io/gh/aviramha/aioaerospike/branch/master/graph/badge.svg)](https://codecov.io/gh/aviramha/aioaerospike)
[![Build Status](https://travis-ci.com/aviramha/aioaerospike.svg?branch=master)](https://travis-ci.com/aviramha/aioaerospike)

This library is planned to be an async API for Aerospike.
The library will be Pure-Python, Protocol based on the C Client.

## Installation
Using pip
```
$ pip install aioaerospike
```

## Contributing

To work on the `aioaerospike` codebase, you'll want to fork the project and clone it locally and install the required dependencies via [poetry](https://poetry.eustace.io):

```sh
$ git clone git@github.com:{USER}/aioaerospike.git
$ make install
```

To run tests and linters use command below (Requires aerospike to run locally on port 3000):

```sh
$ make lint && make test
```

If you want to run only tests or linters you can explicitly specify which test environment you want to run, e.g.:

```sh
$ make lint-black
```

## License

`aioaerospike` is licensed under the MIT license. See the license file for details.

# Latest changes

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

## 0.1.3 (2019-12-07)
- Changed all enums to uppercase
- Added tests for all supported key types
- Added support for dict and list as values.

## 0.1.2 (2019-12-07)
- Fixed key digest, key type can be all supported types (int, float, str, bytes)

## 0.1.1 (2019-12-07)
- Fixed license and metadata


## This package is 3rd party, unrelated to Aerospike company
