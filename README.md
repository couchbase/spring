spring
======

Simple Couchbase CRUD-workload generator based on pylibcouchbase

Requirements:
-------------

* Python 2.6 or higher
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
          -v             show program's version number and exit
          -c             percentage of "create" operations (100 by default)
          -r             percentage of "read" operations (0 by default)
          -u             percentage of "update" operations (0 by default)
          -d             percentage of "delete" operations (0 by default)
          -o             total number of operations (infinity by default)
          -s             average value size in bytes (2048 by default)
          -i             number of existing items (0 by default)
          -w             fractional ratio of working set (1.0 by default)
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

        spring -r 100 -i 2000 -w 8

Delete 2K items:

        spring -d 100 -o 2000
