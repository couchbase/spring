import math
import random
import string


class DocGen(object):

    CHARS = list(string.letters + string.digits)
    SIZE_VARIATION = 0.25  # 25%

    def __init__(self, avg_size):
        self.avg_size = avg_size

    def __iter__(self):
        return self

    @classmethod
    def _get_variation_coeff(cls):
        return random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @classmethod
    def _get_string(cls, length):
        l_int = int(length)
        num_slices = int(math.ceil(length / len(cls.CHARS)))

        random.shuffle(cls.CHARS)  # TODO: this is slow
        rand_chars = num_slices * cls.CHARS

        return ''.join(rand_chars)[:l_int]

    def next(self):
        next_length = self._get_variation_coeff() * self.avg_size
        return {self._get_string(10): self._get_string(next_length)}
