from couchbase import Couchbase
from couchbase.exceptions import NotFoundError


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, **kwargs)

    def create(self, key, doc):
        self.client.set(key, doc)

    def read(self, key):
        self.client.get(key, quiet=True)

    def update(self, key, doc):
        self.client.set(key, doc)

    def delete(self, key):
        try:
            self.client.delete(key)
        except NotFoundError:
            pass
