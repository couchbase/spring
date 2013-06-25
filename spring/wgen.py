import random
from multiprocessing import Process, Value

from couchbase.exceptions import ValueFormatError
from logger import logger

from spring.cbgen import CBGen
from spring.docgen import ExistingKey, KeyForRemoval, NewDocument


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings):
        self.ws = workload_settings
        self.ts = target_settings

    def _gen_sequence(self):
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes
        random.shuffle(ops)
        return ops

    def _report_progress(self, ops, curr_ops):
        if ops < float('inf') and curr_ops > self.next_report * ops:
            progress = 100.0 * curr_ops / ops
            self.next_report += 0.05
            logger.info('Current progress: {0:.2f} %'.format(progress))

    def _do_batch(self, curr_items, deleted_items):
        curr_items_tmp = curr_items.value
        curr_items.value += self.ws.creates
        deleted_items_tmp = deleted_items.value
        deleted_items.value += self.ws.deletes

        for i, op in enumerate(self._gen_sequence()):
            if op == 'c':
                curr_items_tmp += 1
                key, doc = self.docs.next(curr_items_tmp)
                self.cb.create(key, doc)
            elif op == 'r':
                key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
                self.cb.read(key)
            elif op == 'u':
                key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
                key, doc = self.docs.next(curr_items_tmp, key)
                self.cb.update(key, doc)
            elif op == 'd':
                deleted_items_tmp += 1
                key = self.key_for_removal.next(deleted_items_tmp)
                self.cb.delete(key)

    def _run_worker(self, sid, ops, curr_ops, curr_items, deleted_items):
        host, port = self.ts.node.split(':')
        self.cb = CBGen(self.ts.bucket, host, port,
                        self.ts.username, self.ts.password)
        self.key_for_removal = KeyForRemoval()
        self.existing_keys = ExistingKey()
        self.docs = NewDocument(self.ws.size)

        self.next_report = 0.05  # report after every 5% of completion
        try:
            logger.info('Started: Worker-{0}'.format(sid))
            while curr_ops.value < ops:
                curr_ops.value += self.BATCH_SIZE
                self._do_batch(curr_items, deleted_items)
                if not sid:  # only first worker
                    self._report_progress(ops, curr_ops.value)
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: Worker-{0}'.format(sid))
        else:
            logger.info('Finished: Worker-{0}'.format(sid))

    def run(self):
        workers = list()
        curr_items = Value('i', self.ws.items)
        deleted_items = Value('i', 0)
        curr_ops = Value('i', 0)

        for sid in range(self.ws.workers):
            worker = Process(
                target=self._run_worker,
                args=(sid, self.ws.ops, curr_ops, curr_items, deleted_items)
            )
            workers.append(worker)

        map(lambda worker: worker.start(), workers)
        map(lambda worker: worker.join(), workers)
