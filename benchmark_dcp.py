#!/usr/bin/env python

import cProfile
import pstats
import StringIO
from multiprocessing import Value, Lock

from spring.wgen import AsyncKVWorker, DcpWorker


workload_settings = type(
    'WorkloadSettings',
    (object, ),
    {
        'creates': 0,
        'reads': 0,
        'updates': 100,
        'deletes': 0,
        'cases': 0,

        'ops': 50000,
        'throughput': float('inf'),

        'size': 2048,
        'items': 10000,
        'expiration': 0,
        'working_set': 100,
        'working_set_access': 100,

        'workers': 1,
        'query_workers': 0,
        'dcp_workers': 0,

        'n1ql': False
        }
    )()

target_settings = type(
    'TargetSettings',
    (object, ),
    {
        'node': '127.0.0.1:8091',
        'bucket': 'default',
        'username': '',
        'password': '',
        'prefix': None,
        }
    )


def run():
    curr_ops = Value('i', 0)
    curr_items = Value('i', workload_settings.items)
    deleted_items = Value('i', 0)
    lock = Lock()

    worker = AsyncKVWorker(workload_settings, target_settings)
    worker.run(sid=0,
               lock=lock,
               curr_ops=curr_ops,
               curr_items=curr_items,
               deleted_items=deleted_items)

    worker = DcpWorker(workload_settings, target_settings)
    worker.run(1, lock)


def profile():
    pr = cProfile.Profile()
    s = StringIO.StringIO()

    pr.enable()
    run()
    pr.disable()

    ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
    ps.reverse_order()
    ps.print_stats()
    ps.dump_stats('profile.prof')


if __name__ == '__main__':
    profile()
