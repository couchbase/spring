from argparse import ArgumentParser

from spring.settings import WorkloadSettings, TargetSettings
from spring.wgen import WorkloadGen


class ArgParser(ArgumentParser):

    PROG = 'spring'
    USAGE = ('%(prog)s [-s SIZE] [-r SET RATIO] [-o #OPS] [w #WORKERS] '
             '[cb://user:pass@host:port/bucket]')
    VERSION = '1.2.0'

    def __init__(self):
        super(ArgParser, self).__init__(
            prog=self.PROG, usage=self.USAGE, version=self.VERSION
        )
        self._add_arguments()

    def _add_arguments(self):
        self.add_argument(
            'uri',  metavar='URI', nargs='?',
            default='couchbase://127.0.0.1:8091/default',
            help='Connection URI'
        )
        self.add_argument(
            '-s', dest='size', type=int, default=2048,
            help='average value size in bytes (2048 by default)'
        )
        self.add_argument(
            '-r', dest='ratio', type=float, default=1.0,
            help='fractional ratio of set operations (1.0 by default)',
        )
        self.add_argument(
            '-i', dest='items', type=int, default=0,
            help='number of existing items (0 by default)',
        )
        self.add_argument(
            '-o', dest='ops', type=int, default=float('inf'),
            help='total number of operations (infinity by default)'
        )
        self.add_argument(
            '-w', dest='workers', type=int, default=1,
            help='number of workers (1 by default)'
        )

    def parse_args(self, *args):
        args = super(ArgParser, self).parse_args()

        if not 0 <= args.ratio <= 1:
            self.error('Invalid ratio. Allowed range is [0.0, 1.0].')

        if args.ops == float('inf') and args.ratio:
            self.error('Infinite loop allowed only for read-only workloads')

        if args.ratio < 1 and not args.items:
            self.error('Trying to read indefinite dataset. '
                       'Please specify number of items in dataset (-i)')

        return args


def main():
    parser = ArgParser()
    args = parser.parse_args()

    ws = WorkloadSettings(args)
    ts = TargetSettings(args.uri)
    wg = WorkloadGen(ws, ts)
    wg.run()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\nFinished workload'
