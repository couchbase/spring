import math
import time
from hashlib import md5
from itertools import cycle

import random
import numpy as np

from spring.states import STATES, NUM_STATES

from fastdocgen import build_achievements


class Iterator(object):

    def __init__(self):
        self.prefix = None

    def __iter__(self):
        return self

    def add_prefix(self, key):
        if self.prefix:
            return '%s-%s' % (self.prefix, key)
        else:
            return key


class ExistingKey(Iterator):

    def __init__(self, working_set, working_set_access, prefix):
        self.working_set = working_set
        self.working_set_access = working_set_access
        self.prefix = prefix

    def next(self, curr_items, curr_deletes):
        num_existing_items = curr_items - curr_deletes
        num_hot_items = int(num_existing_items * self.working_set / 100.0)
        num_cold_items = num_existing_items - num_hot_items

        left_limit = 1 + curr_deletes
        if self.working_set_access == 100 or \
                random.randint(0, 100) <= self.working_set_access:
            left_limit += num_cold_items
            right_limit = curr_items
        else:
            right_limit = left_limit + num_cold_items
        key = np.random.random_integers(left_limit, right_limit)
        key = '%012d' % key
        return self.add_prefix(key)


class SequentialHotKey(Iterator):

    def __init__(self, sid, ws, prefix):
        self.sid = sid
        self.ws = ws
        self.prefix = prefix

    def __iter__(self):
        num_hot_keys = int(self.ws.items * self.ws.working_set / 100.0)
        num_cold_items = self.ws.items - num_hot_keys

        for seq_id in xrange(1 + num_cold_items + self.sid,
                             1 + self.ws.items,
                             self.ws.workers):
            key = '%012d' % seq_id
            key = self.add_prefix(key)
            yield key


class NewKey(Iterator):

    def __init__(self, prefix, expiration):
        self.prefix = prefix
        self.expiration = expiration
        self.ttls = cycle(range(150, 450, 30))

    def next(self, curr_items):
        key = '%012d' % curr_items
        key = self.add_prefix(key)
        ttl = None
        if self.expiration and random.randint(1, 100) <= self.expiration:
            ttl = self.ttls.next()
        return key, ttl


class KeyForRemoval(Iterator):

    def __init__(self, prefix):
        self.prefix = prefix

    def next(self, curr_deletes):
        key = '%012d' % curr_deletes
        return self.add_prefix(key)


class NewDocument(Iterator):

    SIZE_VARIATION = 0.25  # 25%
    KEY_LENGTH = 10

    def __init__(self, avg_size):
        self.avg_size = avg_size

    @classmethod
    def _get_variation_coeff(cls):
        return np.random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @staticmethod
    def _build_alphabet(key):
        return md5(key).hexdigest() + md5(key[::-1]).hexdigest()

    @staticmethod
    def _build_name(alphabet):
        return '%s %s' % (alphabet[:6], alphabet[6:12])

    @staticmethod
    def _build_email(alphabet):
        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    @staticmethod
    def _build_city(alphabet):
        return alphabet[24:30]

    @staticmethod
    def _build_realm(alphabet):
        return alphabet[30:36]

    @staticmethod
    def _build_country(alphabet):
        return alphabet[42:48]

    @staticmethod
    def _build_county(alphabet):
        return alphabet[48:54]

    @staticmethod
    def _build_street(alphabet):
        return alphabet[54:62]

    @staticmethod
    def _build_coins(alphabet):
        return max(0.1, int(alphabet[36:40], 16) / 100.0)

    @staticmethod
    def _build_gmtime(alphabet):
        seconds = 396 * 24 * 3600 * (int(alphabet[63], 16) % 12)
        return tuple(time.gmtime(seconds))

    @staticmethod
    def _build_year(alphabet):
        return 1985 + int(alphabet[62], 16)

    @staticmethod
    def _build_state(alphabet):
        idx = alphabet.find('7') % NUM_STATES
        return STATES[idx][0]

    @staticmethod
    def _build_full_state(alphabet):
        idx = alphabet.find('8') % NUM_STATES
        return STATES[idx][1]

    @staticmethod
    def _build_category(alphabet):
        return int(alphabet[41], 16) % 3

    @staticmethod
    def _build_achievements(alphabet):
        return build_achievements(alphabet) or [0]

    @staticmethod
    def _build_body(alphabet, length):
        length_int = int(length)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        return body[:length_int]

    def next(self, key):
        next_length = self._get_variation_coeff() * self.avg_size
        alphabet = self._build_alphabet(key)
        return {
            'name': self._build_name(alphabet),
            'email': self._build_email(alphabet),
            'city': self._build_city(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet),
            'body': self._build_body(alphabet, next_length)
        }


class NewNestedDocument(NewDocument):

    OVERHEAD = 450  # Minimum size due to fixed fields, body size is variable

    def _size(self):
        if self.avg_size <= self.OVERHEAD:
            return 0
        if random.random() < 0.975:
            # Normal distribution with mean=self.avg_size
            normal = np.random.normal(loc=1.0, scale=0.17)
            return (self.avg_size - self.OVERHEAD) * normal
        else:
            # Beta distribution, 2KB-2MB range
            return 2048 / np.random.beta(a=2.2, b=1.0)

    def next(self, key):
        alphabet = self._build_alphabet(key)
        size = self._size()
        return {
            'name': {'f': {'f': {'f': self._build_name(alphabet)}}},
            'email': {'f': {'f': self._build_email(alphabet)}},
            'street': {'f': {'f': self._build_street(alphabet)}},
            'city': {'f': {'f': self._build_city(alphabet)}},
            'county': {'f': {'f': self._build_county(alphabet)}},
            'state': {'f': self._build_state(alphabet)},
            'full_state': {'f': self._build_full_state(alphabet)},
            'country': {'f': self._build_country(alphabet)},
            'realm': {'f': self._build_realm(alphabet)},
            'coins': {'f': self._build_coins(alphabet)},
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
        }
