from urlparse import urlparse

from logger import logger


class WorkloadSettings(object):

    def __init__(self, options):
        self.size = options.size
        self.ratio = options.ratio
        self.items = options.items
        self.ops = options.ops
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
