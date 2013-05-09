spring
======

Simple Couchbase workload generator based on pylibcouchbase

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
        Usage: sping [-n host:port] [-u user] [-p pass] [-b bucket] [-s size] [-r set ratio] [-o #ops] [w #workers]

        Options:
          -h, --help         show this help message and exit
          -n 127.0.0.1:8091  node address (host:port)
          -u Administrator   REST username
          -p password        REST password
          -b bucket          bucket name
          -s 2048            average value size in bytes (2048 by default)
          -r 1.0             fractional ratio of set operations (1.0 by default)
          -i 1000000         number of existing items in dataset (0 by default)
          -o 1000            total number of operations (infinity by default)
          -w 10              number of workers (1 by default)

Examples
--------

Insert 1K items:

        spring -o 1000

Update them:

        spring -o 1000

Add 1K more items:

        spring -i 1000 -o 1000

Perform 1K mixed (read/write) operations:

        spring -r 0.5 -i 2000 -o 1000

Infinite read loop using 8 workers:

        spring -r 0.0 -i 2000 -w 8
