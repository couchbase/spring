#!/usr/bin/env python

import cProfile
import pstats
import StringIO

from spring.docgen import NewNestedDocument, NewKey


def generate_keys():
    new_keys = NewKey(prefix=None)
    return tuple(new_keys.next(i)[0] for i in xrange(50000))


def run(keys):
    docs = NewNestedDocument(avg_size=1024)
    for key in keys:
        docs.next(key)


def profile():
    pr = cProfile.Profile()
    s = StringIO.StringIO()

    keys = generate_keys()
    pr.enable()
    run(keys)
    pr.disable()

    ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
    ps.reverse_order()
    ps.print_stats()
    ps.dump_stats('profile.prof')


if __name__ == '__main__':
    profile()
