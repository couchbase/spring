from itertools import cycle
from numpy import random

from couchbase.views.params import Query


class NewQuery(object):

    PARAMS = {
        'limit': 30,
        'stale': 'update_after',
    }

    QUERIES_PER_VIEW = {
        'id_by_city': 9,
        'name_and_email_by_category_and_and_coins': 6,
        'id_by_realm_and_coins': 5,
        'name_and_email_by_city': 9,
        'name_by_category_and_and_coins': 6,
        'experts_id_by_realm_and_coins': 5,
        'id_by_realm': 9,
        'achievements_by_category_and_and_coins': 6,
        'name_and_email_by_realm_and_coins': 5,
        'experts_coins_by_name': 9,
    }

    def __init__(self, ddocs, params):
        self.params = dict(self.PARAMS, **params)

        self.view_sequence = []
        for ddoc_name, ddoc in ddocs.iteritems():
            for view_name in ddoc['views']:
                self.view_sequence += \
                    [(ddoc_name, view_name)] * self.QUERIES_PER_VIEW[view_name]
        random.shuffle(self.view_sequence)
        self.view_sequence = cycle(self.view_sequence)

    @staticmethod
    def generate_params(category, city, realm, name, coins, **kwargs):
        return {
            'id_by_city': {
                'key': city,
            },
            'name_and_email_by_city': {
                'key': city,
            },
            'id_by_realm': {
                'startkey': realm,
            },
            'experts_coins_by_name': {
                'startkey': name,
                'descending': True,
            },
            'name_by_category_and_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'name_and_email_by_category_and_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'achievements_by_category_and_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'id_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
            'name_and_email_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
            'experts_id_by_realm_and_coins': {
                'startkey': [realm, coins],
                'endkey': [realm, 10000],
            },
        }

    def next(self, doc):
        ddoc_name, view_name = self.view_sequence.next()
        params = self.generate_params(**doc)[view_name]
        params = dict(self.params, **params)
        return ddoc_name, view_name, Query(**params)
