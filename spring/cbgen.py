from couchbase.exceptions import (ConnectError,
                                  CouchbaseError,
                                  HTTPError,
                                  KeyExistsError,
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
            TemporaryFailError, TimeoutError) as e:
        logger.warn(e)


class CBGen(object):

    def __init__(self, **kwargs):
        self.client = Connection(quiet=True, timeout=60, **kwargs)
        self.pipeline = self.client.pipeline()

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

    @quiet
    def query(self, ddoc, view, query):
        return tuple(self.client.query(ddoc, view, query=query))
