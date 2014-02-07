from random import choice

import requests
from couchbase.exceptions import (ConnectError,
                                  CouchbaseError,
                                  HTTPError,
                                  KeyExistsError,
                                  NotFoundError,
                                  TemporaryFailError,
                                  TimeoutError,
                                  )
from couchbase.connection import Connection
from decorator import decorator
from logger import logger


@decorator
def quiet(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (ConnectError, CouchbaseError, HTTPError, KeyExistsError,
            NotFoundError, TemporaryFailError, TimeoutError) as e:
        logger.warn('{}: {}'.format(method, e))


class CBGen(object):

    def __init__(self, **kwargs):
        self.client = Connection(timeout=60, quiet=True, **kwargs)
        self.pipeline = self.client.pipeline()
        self.session = requests.Session()
        self.session.auth = (kwargs['username'], kwargs['password'])

    @quiet
    def create(self, key, doc, ttl=None):
        extra_params = {}
        if ttl is None:
            extra_params['ttl'] = ttl
        return self.client.set(key, doc, **extra_params)

    @quiet
    def read(self, key):
        return self.client.get(key)

    @quiet
    def update(self, key, doc):
        return self.client.set(key, doc)

    @quiet
    def cas(self, key, doc):
        cas = self.client.get(key).cas
        return self.client.set(key, doc, cas=cas)

    @quiet
    def delete(self, key):
        return self.client.delete(key)

    def query(self, ddoc, view, query):
        node = choice(self.client.server_nodes).replace("8091", "8092")
        url = "http://{}/{}/_design/{}/_view/{}?{}".format(
            node, self.client.bucket, ddoc, view, query.encoded
        )
        resp = self.session.get(url=url)
        return resp.text

    @quiet
    def lcb_query(self, ddoc, view, query):
        return tuple(self.client.query(ddoc, view, query=query))
