from urlparse import urlparse

from logger import logger


class WorkloadSettings(object):

    def __init__(self, options):
        self.creates = options.creates
        self.reads = options.reads
        self.updates = options.updates
        self.deletes = options.deletes
        self.ops = options.ops
        self.throughput = options.throughput

        self.size = options.size
        self.items = options.items
        self.working_set = options.working_set

        self.workers = options.workers


class TargetSettings(object):

    def __init__(self, target_uri):
        params = urlparse(target_uri)
        if not params.hostname or not params.port or not params.path:
            logger.interrupt('Invalid connection URI')

        self.node = '{0}:{1}'.format(params.hostname, params.port)
        self.bucket = params.path[1:]
        self.username = params.username or ''
        self.password = params.password or ''
        self.prefix = None
