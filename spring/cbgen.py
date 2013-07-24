from couchbase import Couchbase


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, quiet=True, timeout=60, **kwargs)

    def create(self, key, doc, ttl=None):
        if ttl is None:
            self.client.set(key, doc)
        else:
            self.client.set(key, doc, ttl=ttl)

    def read(self, key):
        self.client.get(key)

    def update(self, key, doc):
        self.client.set(key, doc)

    def delete(self, key):
        self.client.delete(key)
