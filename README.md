spring
======

Simple Couchbase CRUD-workload generator based on pylibcouchbase

Requirements:
-------------

* Python 2.7 (including headers)
* libcouchbase
* pip or equivalent

Installation
------------

        pip install spring

Usage
-----

        $ spring -h
        usage: spring [-crud PERCENTAGE] [-o #OPS] [-i #ITEMS] [-n #WORKERS] [cb://user:pass@host:port/bucket]

        positional arguments:
          URI            Connection URI

        optional arguments:
          -h, --help     show this help message and exit
          -v, --version  show program's version number and exit
          -c             percentage of "create" operations (0 by default)
          -r             percentage of "read" operations (0 by default)
          -u             percentage of "update" operations (0 by default)
          -d             percentage of "delete" operations (0 by default)
          -e             percentage of new items that expire (0 by default)
          -o             total number of operations (infinity by default)
          -t             target operations throughput (infinity by default)
          -s             average value size in bytes (2048 by default)
          -i             number of existing items (0 by default)
          -w             percentage of items in working set, 100 by default
          -W             percentage of operations that hit working set, 100 by default
          -n             number of workers (1 by default)

Examples
--------

Insert 1K items:

        spring -c 100 -o 1000

Update them:

        spring -u 100 -o 1000 -i 1000

Add 1K more items:

        spring -c 100 -o 1000 -i 1000

Perform 1K mixed (read/update) operations:

        spring -c 50 -u 50 -o 1000 -i 2000

Infinite read loop using 8 workers:

        spring -r 100 -i 2000 -n 8

Delete 2K items:

        spring -d 100 -o 2000

Running in asynchronous mode:

        spring -r 100 -i 2000 --async

Running benchmarks
------------------

First of all install gprof2dot:

        pip install gprof2dot

Then run one of benchmarks:

        ./benchmark_docs.py && gprof2dot -f pstats profile.prof | dot -Tsvg -o profile.svg

Running unit tests
------------------

After nose installation:

        nosetests -v tests.py

Documents specification
-----------------------

| Field        | Specification            | Combinations           | Example                         |
| -------------| -------------------------| -----------------------| ------------------------------- |
| name         | 12 hex chars             | 281 474 976 710 656    | ecdb3e e921c9                   |
| email        | 12 hex chars             | 281 474 976 710 656    | 3d13c6@a2d1f3.com               |
| street       | 8 hex chars              | 4 294 967 296          | 400f1d0a                        |
| city         | 6 hex chars              | 16 777 216             | 90ac48                          |
| county       | 6 hex chars              | 16 777 216             | 40efd6                          |
| country      | 6 hex chars              | 16 777 216             | 1811db                          |
| realm        | 6 hex chars              | 16 777 216             | 15e3f5                          |
| state        | 2 chars                  | 57                     | WY                              |
| full_state   | 4-24 chars               | 57                     | Montana                         |
| coins        | float [0.1, 655.35]      | 65 535                 | 213.54                          |
| category     | int [0, 2]               | 3                      | 1                               |
| achievements | [1, 10]array int[0, 511] | inf                    | [0, 135, 92]                    |
| gmtime       | date time array          | 12                     | [1972, 3, 3, 0, 0, 0, 4, 63, 0] |
| year         | int [1985, 2000]         | 15                     | 1989                            |
| body         | hex string               | inf                    | N/A                             |
