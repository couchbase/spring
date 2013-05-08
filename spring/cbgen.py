from couchbase import Couchbase


class CBGen(object):

    def __init__(self, host, port, username, password, bucket):
        self.client = Couchbase.connect(host, port, username, password, bucket)

    def _do_get(self, key):
        self.client.get(key, quiet=True)

    def _do_set(self, key, doc):
        self.client.set(key, doc)
