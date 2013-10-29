import random
import time
from multiprocessing import Process, Value, Lock

from couchbase.exceptions import ValueFormatError
from logger import logger

from spring.cbgen import CBGen
from spring.docgen import (ExistingKey, KeyForRemoval, SequentialHotKey,
                           NewKey, NewDocument)
from spring.querygen import NewQuery


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


class Worker(object):

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event

        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         self.ts.prefix)
        self.new_keys = NewKey(self.ts.prefix, self.ws.expiration)
        self.keys_for_removal = KeyForRemoval(self.ts.prefix)
        self.docs = NewDocument(self.ws.size)

        self.next_report = 0.05  # report after every 5% of completion

        self.init_db()

    def init_db(self):
        try:
            host, port = self.ts.node.split(':')
            self.cb = CBGen(bucket=self.ts.bucket, host=host, port=port,
                            username=self.ts.bucket, password=self.ts.password)
        except Exception as e:
            raise SystemExit(e)

    def report_progress(self, curr_ops):
        if self.ws.ops < float('inf') and \
                curr_ops > self.next_report * self.ws.ops:
            progress = 100.0 * curr_ops / self.ws.ops
            self.next_report += 0.05
            logger.info('Current progress: {0:.2f} %'.format(progress))

    def is_time_to_stop(self):
        return (self.shutdown_event is not None and
                self.shutdown_event.is_set())


class KVWorker(Worker):

    def gen_sequence(self):
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['cas'] * self.ws.cases
        random.shuffle(ops)
        return ops

    @with_sleep
    def do_batch(self, curr_items, deleted_items, lock, target_time):
        curr_items_tmp = curr_items.value
        deleted_items_tmp = deleted_items.value
        if self.ws.creates:
            with lock:
                curr_items.value += self.ws.creates
        if self.ws.deletes:
            with lock:
                deleted_items.value += self.ws.deletes

        for op in self.gen_sequence():
            if op == 'c':
                curr_items_tmp += 1
                key, ttl = self.new_keys.next(curr_items_tmp)
                doc = self.docs.next(key)
                self.cb.create(key, doc, ttl)
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
            elif op == 'cas':
                key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
                doc = self.docs.next(key)
                self.cb.cas(key, doc)

    def run(self, sid, lock, curr_ops, curr_items, deleted_items):
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
                self.do_batch(curr_items, deleted_items, lock,
                              target_time=target_time)
                if not sid:  # only first worker
                    self.report_progress(curr_ops.value)
                if self.is_time_to_stop():
                    break
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: worker-{0}'.format(sid))
        else:
            logger.info('Finished: worker-{0}'.format(sid))


class SeqReadsWorker(KVWorker):

    def run(self, sid, *args, **kwargs):
        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            self.cb.read(key)


class SeqUpdatesWorker(KVWorker):

    def run(self, sid, *args, **kwargs):
        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            self.cb.update(key, doc)


class WorkerFactory(object):

    def __new__(self, seq_updates, seq_reads):

        if seq_updates:
            return SeqUpdatesWorker
        if seq_reads:
            return SeqReadsWorker
        if not (seq_updates or seq_reads):
            return KVWorker


class QueryWorker(Worker):

    def __init__(self, workload_settings, target_settings, shutdown_event,
                 ddocs):
        super(QueryWorker, self).__init__(workload_settings, target_settings,
                                          shutdown_event)
        self.new_queries = NewQuery(ddocs)

    @with_sleep
    def do_batch(self, curr_items, deleted_items, target_time):
        curr_items_tmp = curr_items.value
        deleted_items_tmp = deleted_items.value

        for _ in xrange(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items_tmp, deleted_items_tmp)
            doc = self.docs.next(key)
            ddoc_name, view_name, query = self.new_queries.next(doc)
            self.cb.query(ddoc_name, view_name, query=query)

    def run(self, sid, lock, curr_queries, curr_items, deleted_items):
        if self.ws.query_throughput < float('inf'):
            target_time = float(self.BATCH_SIZE) * self.ws.query_workers / \
                self.ws.query_throughput
        else:
            target_time = None
        try:
            logger.info('Started: query-worker-{0}'.format(sid))
            while curr_queries.value < self.ws.ops:
                with lock:
                    curr_queries.value += self.BATCH_SIZE
                self.do_batch(curr_items, deleted_items,
                              target_time=target_time)
                if not sid:  # only first worker
                    self.report_progress(curr_queries.value)
                if self.is_time_to_stop():
                    break
        except (KeyboardInterrupt, ValueFormatError, AttributeError):
            logger.info('Interrupted: query-worker-{0}'.format(sid))
        else:
            logger.info('Finished: query-worker-{0}'.format(sid))


class WorkloadGen(object):

    def __init__(self, workload_settings, target_settings, shutdown_event=None,
                 ddocs=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event
        self.ddocs = ddocs

    def start_kv_workers(self, curr_items, deleted_items):
        curr_ops = Value('i', 0)
        lock = Lock()

        worker_type = WorkerFactory(self.ws.seq_updates, self.ws.seq_reads)
        self.kv_workers = list()
        for sid in range(self.ws.workers):
            worker = worker_type(self.ws, self.ts, self.shutdown_event)
            worker_process = Process(
                target=worker.run,
                args=(sid, lock, curr_ops, curr_items, deleted_items)
            )
            worker_process.start()
            self.kv_workers.append(worker_process)

    def start_query_workers(self, curr_items, deleted_items):
        curr_queries = Value('i', 0)
        lock = Lock()

        self.query_workers = list()
        for sid in range(self.ws.query_workers):
            worker = QueryWorker(self.ws, self.ts, self.shutdown_event,
                                 self.ddocs)
            worker_process = Process(
                target=worker.run,
                args=(sid, lock, curr_queries, curr_items, deleted_items)
            )
            worker_process.start()
            self.query_workers.append(worker_process)

    def wait_for_workers(self, workers):
        for worker in workers:
            worker.join()
            if worker.exitcode:
                logger.interrupt('Worker finished with non-zero exit code')

    def run(self):
        curr_items = Value('i', self.ws.items)
        deleted_items = Value('i', 0)

        self.start_kv_workers(curr_items, deleted_items)
        self.start_query_workers(curr_items, deleted_items)

        self.wait_for_workers(self.kv_workers)
        self.wait_for_workers(self.query_workers)
