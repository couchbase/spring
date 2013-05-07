import math
import random
import string


class DocGen(object):

    CHARS = list(string.letters + string.digits)
    SIZE_VARIATION = 0.25  # 25%
    KEY_LENGTH = 10

    def __init__(self, avg_size):
        self.avg_size = avg_size

    def __iter__(self):
        return self

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
        key = self._build_short_string()
        doc = {key: self._build_long_string(next_length)}
        return key, doc
