from optparse import OptionParser

from spring.settings import WorkloadSettings, TargetSettings
from spring.wgen import WorkloadGen


def get_parser():
    usage = ('%prog [-n host:port] [-u user] [-p pass] [-b bucket] '
             '[-s size] [-r set ratio] [-o #ops] [w #workers]')

    parser = OptionParser(usage)
    parser.prog = 'sping'

    parser.add_option('-n', dest='node', default='127.0.0.1:8091',
                      help='node address (host:port)',
                      metavar='127.0.0.1:8091')
    parser.add_option('-u', dest='username', default='',
                      help='REST username', metavar='Administrator')
    parser.add_option('-p', dest='password', default='',
                      help='REST password', metavar='password')
    parser.add_option('-b', dest='bucket', default='default',
                      help='bucket name', metavar='bucket')

    parser.add_option('-s', dest='size', default=2048, type='int',
                      help='average value size in bytes (2048 by default)',
                      metavar=2048)
    parser.add_option('-r', dest='ratio', type='float', default=1.0,
                      help='fractional ratio of set operations (1.0 by default)',
                      metavar=1.0)
    parser.add_option('-i', dest='items', type='int', default=0,
                      help='number of existing items in dataset (0 by default)',
                      metavar='1000000')
    parser.add_option('-o', dest='ops', type='int', default=float('inf'),
                      help='total number of operations (infinity by default)',
                      metavar='1000')
    parser.add_option('-w', dest='workers', default=1, type="int",
                      help='number of workers (1 by default)', metavar='10')

    return parser


def parse_args(parser):
    options, args = parser.parse_args()

    if not 0 <= options.ratio <= 1:
        parser.error('Invalid ratio. Allowed range is [0.0, 1.0].')

    if options.ops == float('inf') and options.ratio:
        parser.error('Infinite loop allowed only for read-only workloads')

    if options.ratio < 1 and not options.items:
        parser.error('Trying to read indefinite dataset. '
                     'Please specify number of items in dataset (-i)')

    return options


def main():
    parser = get_parser()
    options = parse_args(parser)

    ws = WorkloadSettings(options)
    ts = TargetSettings(options)
    wg = WorkloadGen(ws, ts)
    wg.run()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\nFinished workload'
