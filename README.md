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
        usage: spring [-s SIZE] [-r SET RATIO] [-i #ITEMS] [-o #OPS] [w #WORKERS] [cb://user:pass@host:port/bucket]

        positional arguments:
          URI            Connection URI

        optional arguments:
          -h, --help     show this help message and exit
          -v, --version  show program's version number and exit
          -s             average value size in bytes (2048 by default)
          -i             number of existing items (0 by default)
          -w             fractional ratio of working set (1.0 by default)
          -o             total number of operations (infinity by default)
          -r             fractional ratio of set operations (1.0 by default)
          -n             number of workers (1 by default)

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
