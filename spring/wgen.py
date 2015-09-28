import time
from multiprocessing import Process, Value, Lock, Event

from dcp import DcpClient, ResponseHandler
from decorator import decorator
from numpy import random
from couchbase.exceptions import ValueFormatError
from logger import logger
from twisted.internet import reactor

from spring.cbgen import CBGen, CBAsyncGen, N1QLGen, SpatialGen
from spring.docgen import (ExistingKey, KeyForRemoval, SequentialHotKey,
                           NewKey, NewDocument, NewNestedDocument,
                           ReverseLookupDocument, NewDocumentFromSpatialFile)
from spring.querygen import (ViewQueryGen, ViewQueryGenByType, N1QLQueryGen,
                             SpatialQueryFromFile)


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

    def __init__(self, workload_settings, target_settings,
                 shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event

        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         self.ts.prefix)
        self.new_keys = NewKey(self.ts.prefix, self.ws.expiration)
        self.keys_for_removal = KeyForRemoval(self.ts.prefix)

        if not hasattr(self.ws, 'doc_gen') or self.ws.doc_gen == 'old':
            extra_fields = False
            if (hasattr(self.ws, 'extra_doc_fields') and
                    self.ws['extra_doc_fields'] == 'yes'):
                extra_fields = True
            self.docs = NewDocument(self.ws.size, extra_fields)
        elif self.ws.doc_gen == 'new':
            self.docs = NewNestedDocument(self.ws.size)
        elif self.ws.doc_gen == 'reverse_lookup':
            isRandom = True
            if self.ts.prefix == 'n1ql':
                isRandom = False
            self.docs = ReverseLookupDocument(self.ws.size,
                                              self.ws.doc_partitions,
                                              isRandom)
        elif self.ws.doc_gen == 'spatial':
            self.docs = NewDocumentFromSpatialFile(
                self.ws.spatial.data,
                self.ws.spatial.dimensionality)

        self.next_report = 0.05  # report after every 5% of completion

        host, port = self.ts.node.split(':')
        self.init_db({'bucket': self.ts.bucket, 'host': host, 'port': port,
                      'username': self.ts.bucket,
                      'password': self.ts.password})

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
            curr_items_spot = (curr_items_tmp -
                               self.ws.creates * self.ws.workers)

        deleted_items_tmp = deleted_spot = 0
        if self.ws.deletes:
            with self.lock:
                self.deleted_items.value += self.ws.deletes
                deleted_items_tmp = self.deleted_items.value - self.ws.deletes
            deleted_spot = (deleted_items_tmp +
                            self.ws.deletes * self.ws.workers)

        if not cb:
            cb = self.cb

        # If a file is used as input for the data, make sure the workers
        # read from the correct file offset
        if hasattr(self.ws, 'spatial') and hasattr(self.ws.spatial, 'data'):
            self.docs.offset = curr_items_tmp

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
            if not self.done and (
                    self.curr_ops.value >= self.ws.ops or self.time_to_stop()):
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
            self.target_time = (self.BATCH_SIZE * self.ws.workers /
                                float(self.ws.throughput))
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
            worker = AsyncKVWorker
        elif getattr(workload_settings, 'seq_updates', False):
            worker = SeqUpdatesWorker
        elif getattr(workload_settings, 'seq_reads', False):
            worker = SeqReadsWorker
        elif not (getattr(workload_settings, 'seq_updates', False) or
                  getattr(workload_settings, 'seq_reads', False)):
            worker = KVWorker
        return worker, workload_settings.workers


class ViewWorkerFactory(object):

    def __new__(self, workload_settings):
        return ViewWorker, workload_settings.query_workers


class SpatialWorkerFactory(object):

    def __new__(self, workload_settings):
        workers = 0
        if hasattr(workload_settings, 'spatial'):
            workers = getattr(workload_settings.spatial, 'workers', 0)
        return SpatialWorker, workers


class QueryWorker(Worker):

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super(QueryWorker, self).__init__(workload_settings, target_settings,
                                          shutdown_event)

    @with_sleep
    def do_batch(self):
        curr_items_spot = \
            self.curr_items.value - self.ws.creates * self.ws.workers
        deleted_spot = \
            self.deleted_items.value + self.ws.deletes * self.ws.workers

        for _ in xrange(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items_spot, deleted_spot)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            ddoc_name, view_name, query = self.new_queries.next(doc)
            self.cb.query(ddoc_name, view_name, query=query)

    def run(self, sid, lock, curr_queries, curr_items, deleted_items):
        self.cb.start_updater()

        if self.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.total_workers / \
                self.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.curr_items = curr_items
        self.deleted_items = deleted_items
        self.curr_queries = curr_queries

        try:
            logger.info('Started: {}-{}'.format(self.name, self.sid))
            while curr_queries.value < self.ws.ops and not self.time_to_stop():
                with lock:
                    curr_queries.value += self.BATCH_SIZE
                self.do_batch()
                self.report_progress(curr_queries.value)
        except (KeyboardInterrupt, ValueFormatError, AttributeError) as e:
            logger.info('Interrupted: {}-{}-{}'.format(self.name, self.sid, e))
        else:
            logger.info('Finished: {}-{}'.format(self.name, self.sid))


class ViewWorker(QueryWorker):

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super(ViewWorker, self).__init__(workload_settings, target_settings,
                                         shutdown_event)
        self.total_workers = self.ws.query_workers
        self.throughput = self.ws.query_throughput
        self.name = 'query-worker'

        if workload_settings.index_type is None:
            self.new_queries = ViewQueryGen(workload_settings.ddocs,
                                            workload_settings.qparams)
        else:
            self.new_queries = ViewQueryGenByType(workload_settings.index_type,
                                                  workload_settings.qparams)


class SpatialWorker(QueryWorker):

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super(QueryWorker, self).__init__(workload_settings, target_settings,
                                          shutdown_event)
        self.total_workers = self.ws.spatial.workers
        self.throughput = self.ws.spatial.throughput
        self.name = 'spatial-worker'

        self.new_queries = SpatialQueryFromFile(
            workload_settings.spatial.queries,
            workload_settings.spatial.dimensionality,
            workload_settings.spatial.view_names,
            workload_settings.spatial.params)

        host, port = self.ts.node.split(':')
        params = {'bucket': self.ts.bucket, 'host': host, 'port': port,
                  'username': self.ts.bucket, 'password': self.ts.password}
        self.cb = SpatialGen(**params)

    @with_sleep
    def do_batch(self):
        for i in xrange(self.BATCH_SIZE):
            offset = self.curr_queries.value - self.BATCH_SIZE + i
            ddoc_name, view_name, query = self.new_queries.next(offset)
            self.cb.query(ddoc_name, view_name, query=query)


class N1QLWorkerFactory(object):

    def __new__(self, workload_settings):
        return N1QLWorker, workload_settings.n1ql_workers


class N1QLWorker(QueryWorker):

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super(QueryWorker, self).__init__(workload_settings, target_settings,
                                          shutdown_event)
        self.new_queries = N1QLQueryGen(workload_settings.n1ql_queries)
        self.total_workers = self.ws.n1ql_workers
        self.throughput = self.ws.n1ql_throughput
        self.name = 'n1ql-worker'

        host, port = self.ts.node.split(':')
        params = {'bucket': self.ts.bucket, 'host': host, 'port': port,
                  'username': self.ts.bucket, 'password': self.ts.password}

        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         'n1ql')

        if self.ws.doc_gen == 'reverse_lookup':
            self.docs = ReverseLookupDocument(self.ws.size,
                                              self.ws.doc_partitions,
                                              False)

        self.cb = N1QLGen(**params)


class DcpWorkerFactory(object):

    def __new__(self, workload_settings):
        return DcpWorker, workload_settings.dcp_workers


class DcpHandler(ResponseHandler):

    def __init__(self):
        ResponseHandler.__init__(self)
        self.count = 0

    def mutation(self, response):
        pass
        self.count += 1

    def deletion(self, response):
        pass
        self.count += 1

    def marker(self, response):
        pass

    def stream_end(self, response):
        pass

    def get_num_items(self):
        return self.count


class DcpWorker(Worker):

    def __init__(self, workload_settings, target_settings,
                 shutdown_event=None):
        super(DcpWorker, self).__init__(workload_settings, target_settings,
                                        shutdown_event)

    def init_db(self, params):
        pass

    def run(self, sid, lock):
        self.sid = sid
        host, port = self.ts.node.split(':')

        try:
            self.handler = DcpHandler()
            self.dcp_client = DcpClient()
            self.dcp_client.connect(host, int(port), self.ts.bucket,
                                    'Administrator', 'password',
                                    self.handler)
        except:
            logger.info('Connection Error: dcp-worker-{}'.format(self.sid))
            return

        logger.info('Started: query-worker-{}'.format(self.sid))
        for vb in range(1024):
            start_seqno = 0
            end_seqno = 18446744073709551615  # 2^64 - 1
            result = self.dcp_client.add_stream(vb, 0, start_seqno, end_seqno,
                                                0, 0, 0)
            if result['status'] != 0:
                logger.warn('Stream failed for vb {} due to error {}'
                            .format(vb, result['status']))

        no_items = 0
        last_item_count = 0
        while no_items < 10:
            time.sleep(1)
            cur_items = self.handler.get_num_items()
            if cur_items == last_item_count:
                no_items += 1
            else:
                no_items = 0
            last_item_count = cur_items

        self.dcp_client.close()

        logger.info('Finished: dcp-worker-{}, read {} items'
                    .format(self.sid, last_item_count))


class WorkloadGen(object):

    def __init__(self, workload_settings, target_settings, timer=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.timer = timer
        self.shutdown_event = timer and Event() or None
        self.workers = {}

    def start_workers(self, worker_factory, name,
                      curr_items=None, deleted_items=None):
        curr_ops = Value('L', 0)
        lock = Lock()

        worker_type, total_workers = worker_factory(self.ws)
        self.workers[name] = list()
        for sid in range(total_workers):
            if curr_items is None and deleted_items is None:
                args = (sid, lock)
            else:
                args = (sid, lock, curr_ops, curr_items, deleted_items)
            worker = worker_type(self.ws, self.ts, self.shutdown_event)
            worker_process = Process(target=worker.run, args=tuple(args))
            worker_process.start()
            self.workers[name].append(worker_process)
            if getattr(self.ws, 'async', False):
                time.sleep(2)

    def wait_for_all_workers(self):
        for workers in self.workers.values():
            for worker in workers:
                worker.join()
                if worker.exitcode:
                    logger.interrupt('Worker finished with non-zero exit code')

    def run(self):
        curr_items = Value('L', self.ws.items)
        deleted_items = Value('L', 0)

        logger.info('Start kv workers')
        self.start_workers(WorkerFactory, 'kv', curr_items, deleted_items)

        if self.ws._section == 'access':
            logger.info('Start query workers')
            self.start_workers(ViewWorkerFactory, 'view', curr_items,
                               deleted_items)
            self.start_workers(N1QLWorkerFactory, 'n1ql', curr_items,
                               deleted_items)
            self.start_workers(DcpWorkerFactory, 'dcp')
            self.start_workers(SpatialWorkerFactory, 'spatial', curr_items,
                               deleted_items)

        if self.timer:
            time.sleep(self.timer)
            self.shutdown_event.set()
        self.wait_for_all_workers()
