import math
import random
import string


class Iterator(object):

    def __iter__(self):
        return self


class RandKeyGen(Iterator):

    def __init__(self, items):
        self.items = items

    def next(self):
        return 'key-{0}'.format(random.randint(1, self.items))


class DocGen(Iterator):

    CHARS = list(string.letters + string.digits)
    SIZE_VARIATION = 0.25  # 25%
    KEY_LENGTH = 10

    def __init__(self, avg_size, offset):
        self.avg_size = avg_size
        self.offset = offset

    @classmethod
    def _get_variation_coeff(cls):
        return random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @classmethod
    def _build_short_string(cls):
        return ''.join(cls.CHARS)[-cls.KEY_LENGTH:]

    @classmethod
    def _build_long_string(cls, length):
        l_int = int(length)
        num_slices = int(math.ceil(length / len(cls.CHARS)))
        rand_chars = num_slices * cls.CHARS
        return ''.join(rand_chars)[:l_int]

    def next(self):
        random.shuffle(self.CHARS)

        next_length = self._get_variation_coeff() * self.avg_size
        self.offset += 1

        key = 'key-{0}'.format(self.offset)
        doc = {self._build_short_string(): self._build_long_string(next_length)}
        return key, doc
