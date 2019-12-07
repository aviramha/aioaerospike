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

## 0.1.2 (2019-12-08)
- Fixed key digest, key type can be all supported types (int, float, str, bytes)

## 0.1.1 (2019-12-07)
- Fixed license and metadata

## 0.1.0 (2019-12-07)

- Initial release.


## This package is 3rd party, unrelated to Aerospike company
