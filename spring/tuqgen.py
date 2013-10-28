from itertools import cycle
import random

from couchbase.views.params import Query

class NewTuq(object):

    LIMIT = 30

    QUERIES_PER_TYPE = {
        'where_range': 2,
        'where_lt': 2,
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
            return 'SELECT %s FROM %s WHERE %s > %s AND %s < %s LIMIT %s'\
                   % (index, self.bucket, index, range[0], index, range[1], NewTuq.LIMIT)
        elif qtype == 'where_lt':
            range = self._get_range(doc[index])
            return 'SELECT %s FROM %s WHERE %s < %s LIMIT %s'\
                   % (index, self.bucket, index, range[1], NewTuq.LIMIT)
        elif qtype == 'where_equal':
            return 'SELECT %s FROM %s WHERE %s = %s' \
                   % (index, self.bucket, index, doc[index])

        return None

    def next(self, doc):
        index, qtype = self.tuq_seq.next()
        return self._generate_tuq(doc, index, qtype)

class NewCBQuery(NewTuq):

    def _to_query_param(self, val):
        return [[128, val]]

    def _get_view_info(self, index):
        return 'ddl_%s_idx' % index, '%s_idx' % index

    def _generate_params(self, doc, index, qtype):
        if qtype == 'where_range':
            range = self._get_range(doc[index])
            return {
                'stale': 'false',
                'startkey': self._to_query_param(range[0]),
                'endkey': self._to_query_param(range[1]),
            }
        if qtype == 'where_lt':
            range = self._get_range(doc[index])
            return {
                'stale': 'false',
                'endkey': self._to_query_param(range[1]),
            }
        elif qtype == 'where_equal':
            return  {
                'stale': 'false',
                'key': self._to_query_param(doc[index]),
            }

    def next(self, doc):
        index, qtype = self.tuq_seq.next()
        ddoc_name, view_name = self._get_view_info(index)
        params = self._generate_params(doc, index, qtype)
        if qtype in ['where_equal',]:
            query = Query(**params)
        else:
            query = Query(limit=NewTuq.LIMIT, **params)
        return ddoc_name, view_name, query