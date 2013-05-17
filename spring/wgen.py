import random
from multiprocessing import Process

from spring.cbgen import CBGen
from spring.docgen import RandKeyGen, DocGen


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings):
        self.ws = workload_settings
        self.ts = target_settings

    def _gen_rw_sequence(self):
        ops = int(self.ws.ratio * self.BATCH_SIZE) * [1] + \
            int((1 - self.ws.ratio) * self.BATCH_SIZE) * [0]
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
        host, port = self.ts.node.split(':')
        cb = CBGen(host, port, self.ts.username, self.ts.password,
                   self.ts.bucket)

        ops_per_worker = self.ws.ops / self.ws.workers
        offset = sid * ops_per_worker + self.ws.items
        rkg = RandKeyGen(self.ws.items)
        dg = DocGen(self.ws.size, offset)

        self._run_workload(cb, ops_per_worker, rkg, dg)

    def run(self):
        workers = list()
        for sid in range(self.ws.workers):
            worker = Process(target=self._run_worker, args=(sid,))
            worker.daemon = False
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
