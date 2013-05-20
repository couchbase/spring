import math
import random
from hashlib import md5


class Iterator(object):

    def __iter__(self):
        return self


class RandKeyGen(Iterator):

    def __init__(self, items):
        self.items = items

    def next(self):
        return 'key-{0}'.format(random.randint(1, self.items))


class DocGen(Iterator):

    SIZE_VARIATION = 0.25  # 25%
    KEY_LENGTH = 10

    def __init__(self, avg_size, offset):
        self.avg_size = avg_size
        self.offset = offset

    @classmethod
    def _get_variation_coeff(cls):
        return random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @staticmethod
    def _build_long_string(key, length):
        alphabet = md5(key).hexdigest() + md5(key[::-1]).hexdigest()

        l_int = int(length)
        num_slices = int(math.ceil(length / len(alphabet)))
        rand_chars = num_slices * alphabet
        return rand_chars[:l_int]

    def next(self):
        next_length = self._get_variation_coeff() * self.avg_size
        self.offset += 1

        key = 'key-{0}'.format(self.offset)
        doc = {'body': self._build_long_string(key, next_length)}
        return key, doc
