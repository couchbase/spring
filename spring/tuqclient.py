import requests

from logger import logger


class RestClient(object):
    def __init__(self, username, password):
        self.auth = (username, password)

    def get(self, **kwargs):
        return requests.get(auth=self.auth, **kwargs)

    def post(self, **kwargs):
        return requests.post(auth=self.auth, **kwargs)

    def put(self, **kwargs):
        return requests.put(auth=self.auth, **kwargs)


class TuqClient(RestClient):

    def __init__(self, bucket, host, username, password, tuq_server):
        super(TuqClient, self).__init__(username, password)
        self.master = host
        self.bucket = bucket
        self.API = 'http://%s/query' % tuq_server
        logger.debug('Initializing tuq API %s' % self.API)

    def create_index(self, name, path):
        logger.info('Creating tuq index %s on %s(%s)' % (name, self.bucket, path))

        data='CREATE INDEX %s ON %s(%s)' % (name, self.bucket, path)
        return self.post(url=self.API, data=data)

    def query(self, tuq):
        logger.info("Firing tuq query: %s" % tuq)

        return self.post(url=self.API, data=tuq)