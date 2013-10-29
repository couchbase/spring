from couchbase import Couchbase
from couchbase.exceptions import (ConnectError, HTTPError, TemporaryFailError,
                                  TimeoutError)

from decorator import decorator
from logger import logger


@decorator
def quiet(method, *args, **kwargs):
    try:
        method(*args, **kwargs)
    except (ConnectError, HTTPError, TemporaryFailError, TimeoutError) as e:
        logger.warn(e)


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, quiet=True, timeout=60, **kwargs)

    @quiet
    def create(self, key, doc, ttl=None):
        if ttl is None:
            self.client.set(key, doc)
        else:
            self.client.set(key, doc, ttl=ttl)

    @quiet
    def read(self, key):
        self.client.get(key)

    @quiet
    def update(self, key, doc):
        self.client.set(key, doc)

    def delete(self, key):
        self.client.delete(key)

    @quiet
    def query(self, ddoc, view, query):
        tuple(self.client.query(ddoc, view, query=query))
