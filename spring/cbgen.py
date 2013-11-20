from couchbase import Couchbase
from couchbase.exceptions import (ConnectError,
                                  CouchbaseError,
                                  HTTPError,
                                  KeyExistsError,
                                  TemporaryFailError,
                                  TimeoutError,
                                  )

from decorator import decorator
from logger import logger


@decorator
def quiet(method, *args, **kwargs):
    try:
        method(*args, **kwargs)
    except (ConnectError, CouchbaseError, HTTPError, KeyExistsError,
            TemporaryFailError, TimeoutError) as e:
        logger.warn(e)


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, quiet=True, timeout=60, **kwargs)

    @quiet
    def create(self, key, doc, ttl=None):
        extra_params = {}
        if ttl is None:
            extra_params['ttl'] = ttl
        self.client.set(key, doc, **extra_params)

    @quiet
    def read(self, key):
        self.client.get(key)

    @quiet
    def update(self, key, doc):
        self.client.set(key, doc)

    @quiet
    def cas(self, key, doc):
        cas = self.client.get(key).cas
        self.client.set(key, doc, cas=cas)

    def delete(self, key):
        self.client.delete(key)

    @quiet
    def query(self, ddoc, view, query):
        tuple(self.client.query(ddoc, view, query=query))
