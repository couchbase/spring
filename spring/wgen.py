import sys
import random
from multiprocessing import Process

from spring.cbgen import CBGen
from spring.docgen import RandKeyGen, DocGen


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, options):
        self.options = options

    def _gen_rw_sequence(self):
        ops = int(self.options.ratio * self.BATCH_SIZE) * [1] + \
            int((1 - self.options.ratio) * self.BATCH_SIZE) * [0]
        random.shuffle(ops)
        return ops

    def _run_workload(self, cb, ops_per_worker, rkg, dg):
        while ops_per_worker > 0:
            for op in self._gen_rw_sequence():
                if op:
                    key, doc = dg.next()
                    cb._do_set(key, doc)
                else:
                    key = rkg.next()
                    cb._do_get(key)
            ops_per_worker -= self.BATCH_SIZE

    def _run_worker(self, sid):
        sys.stderr = open('/dev/null', 'w')

        host, port = self.options.node.split(':')
        cb = CBGen(host, port, self.options.username, self.options.password,
                   self.options.bucket)

        ops_per_worker = self.options.ops / self.options.workers
        offset = sid * ops_per_worker + self.options.items
        rkg = RandKeyGen(self.options.items)
        dg = DocGen(self.options.size, offset)

        self._run_workload(cb, ops_per_worker, rkg, dg)

    def run(self):
        workers = list()
        for sid in range(self.options.workers):
            worker = Process(target=self._run_worker, args=(sid,))
            worker.daemon = False
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
