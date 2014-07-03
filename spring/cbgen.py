from random import choice
from threading import Thread
from time import time, sleep

from couchbase import experimental
experimental.enable()
from couchbase.exceptions import (ConnectError,
                                  CouchbaseError,
                                  HTTPError,
                                  KeyExistsError,
                                  NotFoundError,
                                  TemporaryFailError,
                                  TimeoutError,
                                  )
from couchbase.connection import Connection
from txcouchbase.connection import Connection as TxConnection

import requests
from decorator import decorator
from logger import logger


@decorator
def quiet(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (ConnectError, CouchbaseError, HTTPError, KeyExistsError,
            NotFoundError, TemporaryFailError, TimeoutError) as e:
        logger.warn('{}: {}'.format(method, e))


class CBAsyncGen(object):

    def __init__(self, **kwargs):
        self.client = TxConnection(quiet=True, timeout=60, **kwargs)

    def create(self, key, doc, ttl=None):
        extra_params = {}
        if ttl is None:
            extra_params['ttl'] = ttl
        return self.client.set(key, doc, **extra_params)

    def read(self, key):
        return self.client.get(key)

    def update(self, key, doc):
        return self.client.set(key, doc)

    def cas(self, key, doc):
        cas = self.client.get(key).cas
        return self.client.set(key, doc, cas=cas)

    def delete(self, key):
        return self.client.delete(key)


class CBGen(CBAsyncGen):

    NODES_UPDATE_INTERVAL = 15

    def __init__(self, **kwargs):
        self.client = Connection(timeout=60, quiet=True, **kwargs)
        self.session = requests.Session()
        self.session.auth = (kwargs['username'], kwargs['password'])
        self.server_nodes = ['{}:{}'.format(kwargs['host'],
                                            kwargs.get('port', 8091))]
        self.nodes_url = 'http://{}:{}/pools/default/buckets/{}/nodes'.format(
            kwargs['host'],
            kwargs.get('port', 8091),
            kwargs['bucket'],
        )
        self.t = Thread(target=self._get_list_of_servers)
        self.t.daemon = True
        self.t.start()

    def _get_list_of_servers(self):
        while True:
            try:
                nodes = self.session.get(self.nodes_url).json()
            except Exception as e:
                logger.warn('Failed to get list of servers: {}'.format(e))
                continue
            self.server_nodes = [n['hostname'] for n in nodes['servers']]
            sleep(self.NODES_UPDATE_INTERVAL)

    @quiet
    def create(self, *args, **kwargs):
        super(CBGen, self).create(*args, **kwargs)

    @quiet
    def read(self, *args, **kwargs):
        super(CBGen, self).read(*args, **kwargs)

    @quiet
    def update(self, *args, **kwargs):
        super(CBGen, self).update(*args, **kwargs)

    @quiet
    def cas(self, *args, **kwargs):
        super(CBGen, self).cas(*args, **kwargs)

    @quiet
    def delete(self, *args, **kwargs):
        super(CBGen, self).delete(*args, **kwargs)

    def query(self, ddoc, view, query):
        node = choice(self.server_nodes).replace('8091', '8092')
        url = 'http://{}/{}/_design/{}/_view/{}?{}'.format(
            node, self.client.bucket, ddoc, view, query.encoded
        )
        t0 = time()
        resp = self.session.get(url=url)
        latency = time() - t0
        return resp.text, latency

    @quiet
    def lcb_query(self, ddoc, view, query):
        return tuple(self.client.query(ddoc, view, query=query))


class N1QLGen(CBGen):

    def __init__(self, **kwargs):
        self.client = Connection(**kwargs)
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'text/plain'})

    def query(self, ddoc_name, view_name, query):
        query = query.format(bucket=self.client.bucket)
        node = choice(self.server_nodes).replace('8091', '8093')
        url = 'http://{}/query'.format(node)
        t0 = time()
        resp = self.session.post(url=url, data=query)
        latency = time() - t0
        return resp.text, latency
