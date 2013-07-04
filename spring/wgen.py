import random
import time
from multiprocessing import Process, Value, Lock

from couchbase.exceptions import ValueFormatError
from logger import logger

from spring.cbgen import CBGen
from spring.docgen import ExistingKey, KeyForRemoval, NewKey, NewDocument


def with_sleep(method):

    CORRECTION_FACTOR = 0.975  # empiric!

    def wrapper(self, *args, **kwargs):
        target_time = kwargs['target_time']
        if target_time is None:
            return method(self, *args, **kwargs)
        else:
            t0 = time.time()
            method(self, *args, **kwargs)
            actual_time = time.time() - t0
            if actual_time < target_time:
                time.sleep(CORRECTION_FACTOR * (target_time - actual_time))
    return wrapper


class WorkloadGen(object):

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event

    def _gen_sequence(self):
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes
        random.shuffle(ops)
        return ops

    def _report_progress(self, curr_ops):
        if self.ws.ops < float('inf') and \
                curr_ops > self.next_report * self.ws.ops:
            progress = 100.0 * curr_ops / self.ws.ops
            self.next_report += 0.05
            logger.info('Current progress: {0:.2f} %'.format(progress))

    @with_sleep
    def _do_batch(self, curr_items, deleted_items, lock, target_time):
        curr_items_tmp = curr_items.value
        deleted_items_tmp = deleted_items.value
        if self.ws.creates:
            with lock:
                curr_items.value += self.ws.creates
        if self.ws.deletes:
            with lock:
                deleted_items.value += self.ws.deletes

        for i, op in enumerate(self._gen_sequence()):
            if op == 'c':
                curr_items_tmp += 1
                key = self.new_keys.next(curr_items_tmp)
                doc = self.docs.next(key)
                self.cb.create(key, doc)
            elif op == 'r':
                key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
                self.cb.read(key)
            elif op == 'u':
                key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
                doc = self.docs.next(key)
                self.cb.update(key, doc)
            elif op == 'd':
                deleted_items_tmp += 1
                key = self.keys_for_removal.next(deleted_items_tmp)
                self.cb.delete(key)

    def _run_worker(self, sid, lock, curr_ops, curr_items, deleted_items):
        try:
            host, port = self.ts.node.split(':')
            self.cb = CBGen(self.ts.bucket, host, port,
                            self.ts.username, self.ts.password)
        except Exception as e:
            raise SystemExit(e)

        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         self.ts.prefix)
        self.new_keys = NewKey(self.ts.prefix)
        self.keys_for_removal = KeyForRemoval(self.ts.prefix)
        self.docs = NewDocument(self.ws.size)

        self.next_report = 0.05  # report after every 5% of completion
        if self.ws.throughput < float('inf'):
            target_time = float(self.BATCH_SIZE) * self.ws.workers / \
                self.ws.throughput
        else:
            target_time = None
        try:
            logger.info('Started: worker-{0}'.format(sid))
            while curr_ops.value < self.ws.ops:
                with lock:
                    curr_ops.value += self.BATCH_SIZE

                self._do_batch(curr_items, deleted_items, lock,
                               target_time=target_time)

                if not sid:  # only first worker
                    self._report_progress(curr_ops.value)

                if self.shutdown_event is not None and \
                        self.shutdown_event.is_set():
                    break
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: worker-{0}'.format(sid))
        else:
            logger.info('Finished: worker-{0}'.format(sid))

    def run(self):
        workers = list()
        curr_items = Value('i', self.ws.items)
        deleted_items = Value('i', 0)
        curr_ops = Value('i', 0)
        lock = Lock()

        for sid in range(self.ws.workers):
            worker = Process(
                target=self._run_worker,
                args=(sid, lock, curr_ops, curr_items, deleted_items)
            )
            worker.start()
            workers.append(worker)

        for worker in workers:
            worker.join()
            if worker.exitcode:
                logger.interrupt('Worker finished with non-zero exit code')
