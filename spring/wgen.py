import random
from multiprocessing import Process

from cbgen import CBGen
from docgen import RandKeyGen, SeqKeyGen, DocGen


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, options):
        self.options = options

    def _gen_rw_sequence(self):
        ops = int(self.options.ratio * self.BATCH_SIZE) * [1] + \
            int((1 - self.options.ratio) * self.BATCH_SIZE) * [0]
        random.shuffle(ops)
        return ops

    def _run_workload(self, cb, ops_per_worker, dg=None, rkg=None, skg=None):
        while ops_per_worker > 0:
            for op in self._gen_rw_sequence():
                if op:
                    key = skg.next()
                    doc = dg.next()
                    cb._do_set(key, doc)
                else:
                    key = rkg.next()
                    cb._do_get(key)
            ops_per_worker -= self.BATCH_SIZE

    def _run_worker(self, sid):
        host, port = self.options.node.split(':')
        cb = CBGen(host, port, self.options.username, self.options.password,
                   self.options.bucket)

        ops_per_worker = self.options.ops / self.options.workers
        offset = sid * ops_per_worker + self.options.items
        dg = DocGen(self.options.size)
        skg = SeqKeyGen(offset)
        rkg = RandKeyGen(self.options.items)

        self._run_workload(cb, ops_per_worker, dg, rkg, skg)

    def run(self):
        workers = list()
        for sid in range(self.options.workers):
            worker = Process(target=self._run_worker, args=(sid,))
            worker.daemon = False
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
