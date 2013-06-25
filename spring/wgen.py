import random
from multiprocessing import Process, Value

from couchbase.exceptions import ValueFormatError
from logger import logger

from spring.cbgen import CBGen
from spring.docgen import ExistingKey, NewDocument


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings):
        self.ws = workload_settings
        self.ts = target_settings

    def _gen_sequence(self):
        num_creates = int(self.ws.ratio * self.BATCH_SIZE)
        num_reads = int((1 - self.ws.ratio) * self.BATCH_SIZE)
        ops = ['c'] * num_creates + ['r'] * num_reads
        random.shuffle(ops)
        return ops, num_creates, num_reads

    def _report_progress(self, ops, curr_ops):
        if ops < float('inf') and curr_ops > self.next_report * ops:
            progress = 100.0 * curr_ops / ops
            self.next_report += 0.05
            logger.info('Current progress: {0:.2f} %'.format(progress))

    def _do_batch(self, curr_items):
        curr_items_tmp = curr_items.value
        seq, num_creates, _ = self._gen_sequence()
        curr_items.value += num_creates
        for i, op in enumerate(seq):
            if op == 'c':
                curr_items_tmp += 1
                key, doc = self.docs.next(curr_items_tmp)
                self.cb._do_set(key, doc)
            elif op == 'r':
                key = self.existing_keys.next(curr_items_tmp)
                self.cb._do_get(key)

    def _run_worker(self, sid, ops, curr_ops, curr_items):
        host, port = self.ts.node.split(':')
        self.cb = CBGen(self.ts.bucket, host, port,
                        self.ts.username, self.ts.password)
        self.existing_keys = ExistingKey()
        self.docs = NewDocument(self.ws.size)

        self.next_report = 0.05  # recurr_opsport after every 5% of completion
        try:
            logger.info('Started: Worker-{0}'.format(sid))
            while curr_ops.value < ops:
                curr_ops.value += self.BATCH_SIZE
                self._do_batch(curr_items)
                if not sid:  # only first worker
                    self._report_progress(ops, curr_ops.value)
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: Worker-{0}'.format(sid))
        else:
            logger.info('Finished: Worker-{0}'.format(sid))

    def run(self):
        workers = list()
        curr_items = Value('i', self.ws.items)
        curr_ops = Value('i', 0)

        for sid in range(self.ws.workers):
            worker = Process(target=self._run_worker,
                             args=(sid, self.ws.ops, curr_ops, curr_items))
            workers.append(worker)

        map(lambda worker: worker.start(), workers)
        map(lambda worker: worker.join(), workers)
