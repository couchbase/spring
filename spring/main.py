from optparse import OptionParser


def main():
    usage = ('spring [-n host:port] -u -p [-b bucket] '
             '[-s size] [-r set ratio] [-o #ops] [w #workers]')

    parser = OptionParser(usage)

    parser.add_option('-n', dest='node', default='127.0.0.1:8091',
                      help='node address (host:port)',
                      metavar='127.0.0.1:8091')
    parser.add_option('-u', dest='username',
                      help='REST username', metavar='Administrator')
    parser.add_option('-p', dest='password',
                      help='REST password', metavar='password')
    parser.add_option('-b', dest='bucket', default='default',
                      help='bucket name', metavar='bucket')

    parser.add_option('-s', dest='size', default=2048, type='int',
                      help='average value size in bytes (2048 by default)',
                      metavar=2048)
    parser.add_option('-r', dest='ratio', type='float', default=1.0,
                      help='fractional ratio of set operations (1.0 by default)',
                      metavar=1.0)
    parser.add_option('-o', dest='ops', type='int', default=float('inf'),
                      help='total number of operations (infinity by default)',
                      metavar='1000')
    parser.add_option('-w', dest='workers', default=1, type="int",
                      help='number of workers (1 by default)', metavar='10')

    options, args = parser.parse_args()

    if not options.username or not options.password:
        parser.error('Missing credentials')


if __name__ == "__main__":
    main()
