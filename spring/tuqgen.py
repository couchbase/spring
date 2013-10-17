from itertools import cycle
import random


class NewTuq(object):

    QUERIES_PER_TYPE = {
        'where_range': 2,
        'where_equal': 2,
        }

    def __init__(self, indexes, bucket):
        self.bucket = bucket
        self.tuq_seq = []
        for index, qtypes in indexes.iteritems():
            for qtype in qtypes:
                self.tuq_seq += [(index, qtype)] * self.QUERIES_PER_TYPE[qtype]
        random.shuffle(self.tuq_seq)
        self.tuq_seq = cycle(self.tuq_seq)

    def _get_range(self, val):
        return [0, val]

    def _generate_tuq(self, doc, index, qtype):
        if qtype == 'where_range':
            range = self._get_range(doc[index])
            return 'SELECT %s FROM %s WHERE %s > %s AND %s < %s'\
                   % (index, self.bucket, index, range[0], index, range[1])
        elif qtype == 'where_equal':
            return 'SELECT %s FROM %s WHERE %s = %s' \
                   % (index, self.bucket, index, doc[index])

        return None

    def next(self, doc):
        index, qtype = self.tuq_seq.next()
        return self._generate_tuq(doc, index, qtype)