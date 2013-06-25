import math
import random
from hashlib import md5


def with_prefix(method):
    def wrapper(self, *args, **kwargs):
        key = method(self, *args, **kwargs)
        if self.prefix is not None:
            key = '{0}-{1}'.format(self.prefix, key)
        return key
    return wrapper


class Iterator(object):

    def __iter__(self):
        return self


class ExistingKey(Iterator):

    def __init__(self, working_set, prefix=None):
        self.working_set = working_set
        self.prefix = prefix

    @with_prefix
    def next(self, curr_items, curr_deletes):
        offset = 1 + curr_deletes + int((1 - self.working_set) * curr_items)
        key = 'key-{0}'.format(random.randint(offset, curr_items))
        return key


class NewKey(Iterator):

    def __init__(self, prefix=None):
        self.prefix = prefix

    @with_prefix
    def next(self, curr_items):
        key = 'key-{0}'.format(curr_items)
        return key


class KeyForRemoval(NewKey):

    @with_prefix
    def next(self, curr_deletes):
        key = 'key-{0}'.format(curr_deletes)
        return key


class NewDocument(Iterator):

    SIZE_VARIATION = 0.25  # 25%
    KEY_LENGTH = 10

    def __init__(self, avg_size):
        self.avg_size = avg_size

    @classmethod
    def _get_variation_coeff(cls):
        return random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @staticmethod
    def _build_alphabet(key):
        return md5(key).hexdigest() + md5(key[::-1]).hexdigest()

    @staticmethod
    def _build_name(alphabet):
        return '{0} {1}'.format(alphabet[:6], alphabet[6:12])

    @staticmethod
    def _build_email(alphabet):
        return '{0}@{1}.com'.format(alphabet[12:18], alphabet[18:24])

    @staticmethod
    def _build_city(alphabet):
        return alphabet[24:30]

    @staticmethod
    def _build_realm(alphabet):
        return alphabet[30:36]

    @staticmethod
    def _build_coins(alphabet):
        return max(0.0, int(alphabet[36:40], 16) / 100.0)

    @staticmethod
    def _build_category(alphabet):
        return int(alphabet[41], 16) % 3

    @staticmethod
    def _build_achievements(alphabet):
        achievement = 256
        achievements = []
        for i, char in enumerate(alphabet[42:58]):
            achievement = (achievement + int(char, 16) * i) % 512
            if achievement < 256:
                achievements.append(achievement)
        return achievements

    @staticmethod
    def _build_body(alphabet, length):
        length_int = int(length)
        num_slices = int(math.ceil(length / len(alphabet)))
        body = num_slices * alphabet
        return body[:length_int]

    def next(self, key):
        next_length = self._get_variation_coeff() * self.avg_size
        alphabet = self._build_alphabet(key)
        doc = {
            'name': self._build_name(alphabet),
            'email': self._build_email(alphabet),
            'city': self._build_city(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet),
            'body': self._build_body(alphabet, next_length)
        }
        return doc
