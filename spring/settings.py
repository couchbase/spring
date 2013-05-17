import sys
from urlparse import urlparse


class WorkloadSettings(object):

    def __init__(self, options):
        self.size = options.size
        self.ratio = options.ratio
        self.items = options.items
        self.ops = options.ops
        self.workers = options.workers


class TargetSettings(object):

    def __init__(self, target_uri):
        params = urlparse(target_uri)
        self.node = '{0}:{1}'.format(params.hostname, params.port)
        self.bucket = params.path[1:]
        self.username = params.username or ''
        self.password = params.password or ''

        if not params.scheme or not self.node or not self.bucket:
            sys.exit('Invalid connection URI')
