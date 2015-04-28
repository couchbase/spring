import json
import unittest

import numpy as np

from spring.docgen import NewNestedDocument, SequentialHotKey
from spring.querygen import N1QLQueryGen

from fastdocgen import build_achievements


def py_build_achievements(alphabet):
    achievement = 256
    achievements = []
    for i, char in enumerate(alphabet[42:58]):
        achievement = (achievement + int(char, 16) * i) % 512
        if achievement < 256:
            achievements.append(achievement)
    return achievements


class FastDocGenTest(unittest.TestCase):

    ALPHABET = '0b1efc8985ca1efb7c1b56a8ec698b87fbdb7b27b6370af9782a48bb587019'

    def test_build_achievements(self):
        py = py_build_achievements(self.ALPHABET)
        c = build_achievements(self.ALPHABET)
        self.assertEqual(py, c)


class NestedDocTest(unittest.TestCase):

    SIZE = 1024

    def test_doc_size(self):
        docgen = NewNestedDocument(avg_size=self.SIZE)
        sizes = tuple(
            len(json.dumps(docgen.next(key='%012s' % i)))
            for i in range(10000)
        )
        mean = np.mean(sizes)
        self.assertAlmostEqual(mean, 1024, delta=128)
        p95 = np.percentile(sizes, 97)
        self.assertLess(p95, 2048)
        p99 = np.percentile(sizes, 98)
        self.assertGreater(p99, 2048)
        self.assertLess(max(sizes), 2 * 1024 ** 2)
        self.assertGreater(min(sizes), 0)

    def test_doc_content(self):
        docgen = NewNestedDocument(avg_size=0)
        actual = docgen.next(key='000000000020')
        expected = {
            'name': {'f': {'f': {'f': 'ecdb3e e921c9'}}},
            'email': {'f': {'f': '3d13c6@a2d1f3.com'}},
            'street': {'f': {'f': '400f1d0a'}},
            'city': {'f': {'f': '90ac48'}},
            'county': {'f': {'f': '40efd6'}},
            'state': {'f': 'WY'},
            'full_state': {'f': 'Montana'},
            'country': {'f': '1811db'},
            'realm': {'f': '15e3f5'},
            'coins': {'f': 213.54},
            'category': 1,
            'achievements': [0, 135, 92],
            'gmtime': (1972, 3, 3, 0, 0, 0, 4, 63, 0),
            'year': 1989,
            'body': '',
        }
        self.assertEqual(actual, expected)

    def test_gmtime_variation(self):
        docgen = NewNestedDocument(avg_size=0)
        keys = set()
        for k in range(1000):
            key = '%012d' % k
            doc = docgen.next(key)
            keys.add(doc['gmtime'])
        self.assertEqual(len(keys), 12)

    def test_achievements_length(self):
        docgen = NewNestedDocument(avg_size=0)
        for k in range(100000):
            key = '%012d' % k
            doc = docgen.next(key)
            self.assertLessEqual(len(doc['achievements']), 10)
            self.assertGreater(len(doc['achievements']), 0)

    def test_determenistic(self):
        docgen = NewNestedDocument(avg_size=self.SIZE)
        d1 = docgen.next(key='mykey')
        d2 = docgen.next(key='mykey')
        d1['body'] = d2['body'] = None
        self.assertEqual(d1, d2)

    def test_alphabet_size(self):
        docgen = NewNestedDocument(avg_size=self.SIZE)
        alphabet = docgen._build_alphabet('key')
        self.assertEqual(len(alphabet), 64)


class KeysTest(unittest.TestCase):

    def test_seq_hot_keys(self):
        ws = type('', (), {'items': 10000, 'working_set': 20, 'workers': 20})()
        hot_keys = [
            '%012d' % i
            for i in range(1 + ws.items * (100 - ws.working_set) / 100,
                           ws.items + 1)
        ]
        actual_keys = []
        for sid in range(ws.workers):
            actual_keys += list(SequentialHotKey(sid=sid, ws=ws, prefix=None))
        self.assertEqual(sorted(hot_keys), sorted(actual_keys))


class N1QlTsts(unittest.TestCase):

    SIZE = 1024

    def test_query_formatting(self):
        docgen = NewNestedDocument(avg_size=self.SIZE)
        doc = docgen.next('test-key')
        queries = ['SELECT * from `bucket-1`;', 'SELECT count(*) from `bucket-1`;']
        qgen = N1QLQueryGen(queries=queries)
        _, _, query = qgen.next(doc)
        query.format(bucket='bucket-1')


if __name__ == '__main__':
    unittest.main()
