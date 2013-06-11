import random
from multiprocessing import Process

from logger import logger

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

    def _report_progress(self, ops_per_worker):
        if ops_per_worker < float('inf') and \
                self.curr_ops > self.next_report * ops_per_worker:
            progress = 100.0 * self.curr_ops / ops_per_worker
            self.next_report += 0.05
            logger.info('Current progress: {0:.2f} %'.format(progress))

    def _run_workload(self, cb, ops_per_worker, rkg, dg, sid):
        self.curr_ops = 0
        self.next_report = 0.05  # report after every 5% of completion
        while self.curr_ops < ops_per_worker:
            for op in self._gen_rw_sequence():
                if op:
                    key, doc = dg.next()
                    cb._do_set(key, doc)
                else:
                    key = rkg.next()
                    cb._do_get(key)
            self.curr_ops += self.BATCH_SIZE
            if not sid:  # only first worker
                self._report_progress(ops_per_worker)

    def _run_worker(self, sid):
        host, port = self.ts.node.split(':')
        cb = CBGen(host, port, self.ts.username, self.ts.password,
                   self.ts.bucket)

        ops_per_worker = self.ws.ops / self.ws.workers
        offset = sid * ops_per_worker + self.ws.items
        working_set = int(self.ws.working_set * self.ws.items)
        rkg = RandKeyGen(working_set, self.ts.prefix)
        dg = DocGen(self.ws.size, offset, self.ts.prefix)

        self._run_workload(cb, ops_per_worker, rkg, dg, sid)

    def run(self):
        workers = list()
        for sid in range(self.ws.workers):
            worker = Process(target=self._run_worker, args=(sid,))
            worker.name = 'Worker-{0}'.format(sid)
            worker.daemon = False
            worker.start()
            workers.append(worker)
            logger.info('Started {0}'.format(worker.name))

        for worker in workers:
            worker.join()
            logger.info('Stopped {0}'.format(worker.name))
