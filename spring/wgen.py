import time
from multiprocessing import Process, Value, Lock, Event

from decorator import decorator
from numpy import random
from couchbase.exceptions import ValueFormatError
from logger import logger
from twisted.internet import reactor

from spring.cbgen import CBGen, CBAsyncGen
from spring.docgen import (ExistingKey, KeyForRemoval, SequentialHotKey,
                           NewKey, NewDocument, NewNestedDocument)
from spring.querygen import NewQuery, NewQueryNG


@decorator
def with_sleep(method, *args):
    self = args[0]
    if self.target_time is None:
        return method(self)
    else:
        t0 = time.time()
        method(self)
        actual_time = time.time() - t0
        delta = self.target_time - actual_time
        if delta > 0:
            time.sleep(self.CORRECTION_FACTOR * delta)


class Worker(object):

    CORRECTION_FACTOR = 0.975  # empiric!

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

        if not hasattr(self.ws, 'doc_gen') or self.ws.doc_gen == 'old':
            self.docs = NewDocument(self.ws.size)
        else:
            self.docs = NewNestedDocument(self.ws.size)

        self.next_report = 0.05  # report after every 5% of completion

        host, port = self.ts.node.split(':')
        self.init_db({'bucket': self.ts.bucket, 'host': host, 'port': port,
                      'username': self.ts.bucket, 'password': self.ts.password})

    def init_db(self, params):
        try:
            self.cb = CBGen(**params)
        except Exception as e:
            raise SystemExit(e)

    def report_progress(self, curr_ops):  # only first worker
        if not self.sid and self.ws.ops < float('inf') and \
                curr_ops > self.next_report * self.ws.ops:
            progress = 100.0 * curr_ops / self.ws.ops
            self.next_report += 0.05
            logger.info('Current progress: {:.2f} %'.format(progress))

    def time_to_stop(self):
        return (self.shutdown_event is not None and
                self.shutdown_event.is_set())


class KVWorker(Worker):

    def gen_cmd_sequence(self, cb=None):
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['cas'] * self.ws.cases
        random.shuffle(ops)

        curr_items_tmp = curr_items_spot = self.curr_items.value
        if self.ws.creates:
            with self.lock:
                self.curr_items.value += self.ws.creates
                curr_items_tmp = self.curr_items.value - self.ws.creates
            curr_items_spot = curr_items_tmp - self.ws.creates * self.ws.workers

        deleted_items_tmp = deleted_spot = 0
        if self.ws.deletes:
            with self.lock:
                self.deleted_items.value += self.ws.deletes
                deleted_items_tmp = self.deleted_items.value - self.ws.deletes
            deleted_spot = deleted_items_tmp + self.ws.deletes * self.ws.workers

        if not cb:
            cb = self.cb

        cmds = []
        for op in ops:
            if op == 'c':
                curr_items_tmp += 1
                key, ttl = self.new_keys.next(curr_items_tmp)
                doc = self.docs.next(key)
                cmds.append((cb.create, (key, doc, ttl)))
            elif op == 'r':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                cmds.append((cb.read, (key, )))
            elif op == 'u':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                doc = self.docs.next(key)
                cmds.append((cb.update, (key, doc)))
            elif op == 'd':
                deleted_items_tmp += 1
                key = self.keys_for_removal.next(deleted_items_tmp)
                cmds.append((cb.delete, (key, )))
            elif op == 'cas':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                doc = self.docs.next(key)
                cmds.append((cb.cas, (key, doc)))
        return cmds

    @with_sleep
    def do_batch(self, *args, **kwargs):
        for cmd, args in self.gen_cmd_sequence():
            cmd(*args)

    def run(self, sid, lock, curr_ops, curr_items, deleted_items):
        if self.ws.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.ws.workers / \
                self.ws.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.lock = lock
        self.curr_items = curr_items
        self.deleted_items = deleted_items

        logger.info('Started: worker-{}'.format(self.sid))
        try:
            while curr_ops.value < self.ws.ops and not self.time_to_stop():
                with lock:
                    curr_ops.value += self.BATCH_SIZE
                self.do_batch()
                self.report_progress(curr_ops.value)
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: worker-{}'.format(self.sid))
        else:
            logger.info('Finished: worker-{}'.format(self.sid))


class AsyncKVWorker(KVWorker):

    NUM_CONNECTIONS = 8

    def init_db(self, params):
        self.cbs = [CBAsyncGen(**params) for _ in range(self.NUM_CONNECTIONS)]
        self.counter = range(self.NUM_CONNECTIONS)

    def restart(self, _, cb, i):
        self.counter[i] += 1
        if self.counter[i] == self.BATCH_SIZE:
            actual_time = time.time() - self.time_started
            if self.target_time is not None:
                delta = self.target_time - actual_time
                if delta > 0:
                    time.sleep(self.CORRECTION_FACTOR * delta)

            self.report_progress(self.curr_ops.value)
            if not self.done and (self.curr_ops.value >= self.ws.ops or self.time_to_stop()):
                with self.lock:
                    self.done = True
                logger.info('Finished: worker-{}'.format(self.sid))
                reactor.stop()
            else:
                self.do_batch(_, cb, i)

    def do_batch(self, _, cb, i):
        self.counter[i] = 0
        self.time_started = time.time()

        with self.lock:
            self.curr_ops.value += self.BATCH_SIZE

        for cmd, args in self.gen_cmd_sequence(cb):
            d = cmd(*args)
            d.addCallback(self.restart, cb, i)
            d.addErrback(self.log_and_restart, cb, i)

    def log_and_restart(self, err, cb, i):
        logger.warn('Request problem with worker-{} thread-{}: {}'.format(
            self.sid, i, err.value)
        )
        self.restart(None, cb, i)

    def error(self, err, cb, i):
        logger.warn('Connection problem with worker-{} thread-{}: {}'.format(
            self.sid, i, err)
        )

        cb.client._close()
        time.sleep(15)
        d = cb.client.connect()
        d.addCallback(self.do_batch, cb, i)
        d.addErrback(self.error, cb, i)

    def run(self, sid, lock, curr_ops, curr_items, deleted_items):
        if self.ws.throughput < float('inf'):
            self.target_time = self.BATCH_SIZE * self.ws.workers / float(self.ws.throughput)
        else:
            self.target_time = None
        self.sid = sid
        self.lock = lock
        self.curr_items = curr_items
        self.deleted_items = deleted_items
        self.curr_ops = curr_ops

        self.done = False
        for i, cb in enumerate(self.cbs):
            d = cb.client.connect()
            d.addCallback(self.do_batch, cb, i)
            d.addErrback(self.error, cb, i)
        logger.info('Started: worker-{}'.format(self.sid))
        reactor.run()


class SeqReadsWorker(Worker):

    def run(self, sid, *args, **kwargs):
        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            self.cb.read(key)


class SeqUpdatesWorker(Worker):

    def run(self, sid, *args, **kwargs):
        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            self.cb.update(key, doc)


class WorkerFactory(object):

    def __new__(self, workload_settings):

        if getattr(workload_settings, 'async', False):
            return AsyncKVWorker
        if getattr(workload_settings, 'seq_updates', False):
            return SeqUpdatesWorker
        if getattr(workload_settings, 'seq_reads', False):
            return SeqReadsWorker
        if not (getattr(workload_settings, 'seq_updates', False) or
                getattr(workload_settings, 'seq_reads', False)):
            return KVWorker


class QueryWorker(Worker):

    def __init__(self, workload_settings, target_settings, shutdown_event,
                 ddocs, params, index_type):
        super(QueryWorker, self).__init__(workload_settings, target_settings,
                                          shutdown_event)
        if index_type is None:
            self.new_queries = NewQuery(ddocs, params)
        else:
            self.new_queries = NewQueryNG(index_type)

    @with_sleep
    def do_batch(self):
        curr_items_spot = \
            self.curr_items.value - self.ws.creates * self.ws.workers
        deleted_spot = \
            self.deleted_items.value + self.ws.deletes * self.ws.workers

        for _ in xrange(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items_spot, deleted_spot)
            doc = self.docs.next(key)
            ddoc_name, view_name, query = self.new_queries.next(doc)
            self.cb.query(ddoc_name, view_name, query=query)

    def run(self, sid, lock, curr_queries, curr_items, deleted_items):
        if self.ws.query_throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.ws.query_workers / \
                self.ws.query_throughput
        else:
            self.target_time = None
        self.sid = sid
        self.curr_items = curr_items
        self.deleted_items = deleted_items

        try:
            logger.info('Started: query-worker-{}'.format(self.sid))
            while curr_queries.value < self.ws.ops and not self.time_to_stop():
                with lock:
                    curr_queries.value += self.BATCH_SIZE
                self.do_batch()
                self.report_progress(curr_queries.value)
        except (KeyboardInterrupt, ValueFormatError, AttributeError):
            logger.info('Interrupted: query-worker-{}'.format(self.sid))
        else:
            logger.info('Finished: query-worker-{}'.format(self.sid))


class WorkloadGen(object):

    def __init__(self, workload_settings, target_settings, timer=None,
                 ddocs=None, qparams={}, index_type=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.timer = timer
        self.shutdown_event = timer and Event() or None

        self.ddocs = ddocs
        self.qparams = qparams
        self.index_type = index_type

    def start_kv_workers(self, curr_items, deleted_items):
        curr_ops = Value('L', 0)
        lock = Lock()

        worker_type = WorkerFactory(self.ws)
        self.kv_workers = list()
        for sid in range(self.ws.workers):
            worker = worker_type(self.ws, self.ts, self.shutdown_event)
            worker_process = Process(
                target=worker.run,
                args=(sid, lock, curr_ops, curr_items, deleted_items)
            )
            worker_process.start()
            self.kv_workers.append(worker_process)
            if getattr(self.ws, 'async', False):
                time.sleep(2)

    def start_query_workers(self, curr_items, deleted_items):
        curr_queries = Value('L', 0)
        lock = Lock()

        self.query_workers = list()
        for sid in range(self.ws.query_workers):
            worker = QueryWorker(self.ws, self.ts, self.shutdown_event,
                                 self.ddocs, self.qparams, self.index_type)
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
        curr_items = Value('L', self.ws.items)
        deleted_items = Value('L', 0)

        self.start_kv_workers(curr_items, deleted_items)
        self.start_query_workers(curr_items, deleted_items)

        if self.timer:
            time.sleep(self.timer)
            self.shutdown_event.set()
        self.wait_for_workers(self.kv_workers)
        self.wait_for_workers(self.query_workers)
