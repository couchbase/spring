from couchbase import Couchbase


class CBGen(object):

    def __init__(self, *args, **kwargs):
        self.client = Couchbase.connect(*args, **kwargs)

    def _do_get(self, key):
        self.client.get(key, quiet=True)

    def _do_set(self, key, doc):
        self.client.set(key, doc)
