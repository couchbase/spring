class WorkloadSettings(object):

    def __init__(self, options):
        self.size = options.size
        self.ratio = options.ratio
        self.items = options.items
        self.ops = options.ops
        self.workers = options.workers


class TargetSettings(object):

    def __init__(self, options):
        self.node = options.node
        self.username = options.username
        self.password = options.password
        self.bucket = options.bucket
