from couchbase import Couchbase


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, **kwargs, quiet=True, timeout=60)

    def create(self, key, doc):
        self.client.set(key, doc)

    def read(self, key):
        self.client.get(key)

    def update(self, key, doc):
        self.client.set(key, doc)

    def delete(self, key):
        self.client.delete(key)
