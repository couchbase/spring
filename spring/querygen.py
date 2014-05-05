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
        'name_and_email_by_category_and_coins': 6,
        'id_by_realm_and_coins': 5,
        'name_and_email_by_city': 9,
        'name_by_category_and_coins': 6,
        'experts_id_by_realm_and_coins': 5,
        'id_by_realm': 9,
        'achievements_by_category_and_coins': 6,
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
            'name_by_category_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'name_and_email_by_category_and_coins': {
                'startkey': [category, 0],
                'endkey': [category, coins],
            },
            'achievements_by_category_and_coins': {
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


class NewQueryNG(object):

    PARAMS = {
        'limit': 20,
        'stale': 'update_after',
    }

    DDOC_NAME = 'ddoc'

    VIEWS_PER_TYPE = {
        'basic': (
            'name_and_street_by_city',
            'name_and_email_by_county',
            'achievements_by_realm',
        ),
        'range': (
            'name_by_coins',
            'email_by_achievement_and_category',
            'street_by_year_and_coins',
        ),
        'group_by': (
            'coins_stats_by_state_and_year',
            'coins_stats_by_gmtime_and_year',
            'coins_stats_by_full_state_and_year',
        ),
        'multi_emits': (
            'name_and_email_and_street_and_achievements_and_coins_by_city',
            'street_and_name_and_email_and_achievement_and_coins_by_county',
            'category_name_and_email_and_street_and_gmtime_and_year_by_country',
        ),
        'compute': (
            'calc_by_city',
            'calc_by_county',
            'calc_by_realm',
        ),
        'body': (
            'body_by_city',
            'body_by_realm',
            'body_by_country',
        ),
    }

    def __init__(self, index_type, params):
        self.params = dict(self.PARAMS, **params)

        self.view_sequence = cycle(self.VIEWS_PER_TYPE[index_type])

    @staticmethod
    def generate_params(city, county, country, realm, state, full_state, coins,
                        category, year, achievements, gmtime, **kwargs):
        return {
            'name_and_street_by_city': {
                'key': city['f']['f'],
            },
            'name_and_email_by_county': {
                'key': county['f']['f'],
            },
            'achievements_by_realm': {
                'key': realm['f'],
            },
            'name_by_coins': {
                'startkey': coins['f'] * 0.5,
                'endkey': coins['f'],
            },
            'email_by_achievement_and_category': {
                'startkey': [0, category],
                'endkey': [achievements[0], category],
            },
            'street_by_year_and_coins': {
                'startkey': [year, coins['f']],
                'endkey': [year, 655.35],
            },
            'coins_stats_by_state_and_year': {
                'key': [state['f'], year],
                'group': 'true'
            },
            'coins_stats_by_gmtime_and_year': {
                'key': [gmtime, year],
                'group_level': 2
            },
            'coins_stats_by_full_state_and_year': {
                'key': [full_state['f'], year],
                'group': 'true'
            },
            'name_and_email_and_street_and_achievements_and_coins_by_city': {
                'key': city['f']['f'],
            },
            'street_and_name_and_email_and_achievement_and_coins_by_county': {
                'key': county['f']['f'],
            },
            'category_name_and_email_and_street_and_gmtime_and_year_by_country': {
                'key': country['f'],
            },
            'calc_by_city': {
                'key': city['f']['f'],
            },
            'calc_by_county': {
                'key': county['f']['f'],
            },
            'calc_by_realm': {
                'key': realm['f'],
            },
            'body_by_city': {
                'key': city['f']['f'],
            },
            'body_by_realm': {
                'key': realm['f'],
            },
            'body_by_country': {
                'key': country['f'],
            },
        }

    def next(self, doc):
        view_name = self.view_sequence.next()
        params = self.generate_params(**doc)[view_name]
        params = dict(self.params, **params)
        return self.DDOC_NAME, view_name, Query(**params)


class NewN1QLQuery(NewQueryNG):

    QUERIES = {
        'name_and_street_by_city': '''
            SELECT category
                FROM {{bucket}}
                WHERE city.f.f = "{city[f][f]}"
                LIMIT 20
        ''',
        'name_and_email_by_county': '''
            SELECT name.f.f.f AS _name, email.f.f AS _email
                FROM {{bucket}}
                WHERE county.f.f = "{county[f][f]}"
                LIMIT 20
        ''',
        'achievements_by_realm': '''
            SELECT achievements
                FROM {{bucket}}
                WHERE realm.f = "{realm[f]}"
                LIMIT 20
        ''',
        'name_by_coins': '''
            SELECT name.f.f.f AS _name
                FROM {{bucket}}
                WHERE coins.f > {coins[f]} AND coins.f < {coins[f]}
                LIMIT 20
        ''',
        'email_by_achievement_and_category': '''
            SELECT email.f.f AS _email
                FROM {{bucket}}
                WHERE category = {category}
                    AND achievements[0] > 0
                    AND achievements[0] < {achievements[0]}
                LIMIT 20
        ''',
        'street_by_year_and_coins': '''
            SELECT street
                FROM {{bucket}}
                WHERE year = {year}
                    AND coins.f > {coins[f]}
                    AND coins.f < 655.35
                LIMIT 20
        ''',
        'name_and_email_and_street_and_achievements_and_coins_by_city': '''
            SELECT name.f.f.f AS _name,
                    email.f.f AS _email,
                    street.f.f AS _street,
                    achievements,
                    coins.f AS _coins
                FROM {{bucket}}
                WHERE city.f.f = "{city[f][f]}"
                LIMIT 20
        ''',
        'street_and_name_and_email_and_achievement_and_coins_by_county': '''
            SELECT street.f.f AS _street,
                    name.f.f.f AS _name,
                    email.f.f AS _email,
                    achievements[0] AS achievement,
                    2*coins.f AS _coins
                FROM {{bucket}}
                WHERE county.f.f = "{county[f][f]}"
                LIMIT 20
        ''',
        'category_name_and_email_and_street_and_gmtime_and_year_by_country': '''
            SELECT category,
                    name.f.f.f AS _name,
                    email.f.f AS _email,
                    street.f.f AS _street,
                    gmtime,
                    year
                FROM {{bucket}}
                WHERE country.f = "{country[f]}"
                LIMIT 20
        ''',
        'body_by_city': '''
            SELECT body
                FROM {{bucket}}
                WHERE city.f.f = "{city[f][f]}"
                LIMIT 20
        ''',
        'body_by_realm': '''
            SELECT body
                FROM {{bucket}}
                WHERE realm.f = "{realm[f]}"
                LIMIT 20
        ''',
        'body_by_country': '''
            SELECT body
                FROM {{bucket}}
                WHERE country.f = "{country[f]}"
                LIMIT 20
        ''',
        'coins_stats_by_state_and_year': '''
            SELECT COUNT(coins.f),
                    SUM(coins.f),
                    AVG(coins.f),
                    MIN(coins.f),
                    MAX(coins.f)
                FROM {{bucket}}
                WHERE state.f = "{state[f]}" and year = {year}
                GROUP BY state.f, year
                LIMIT 20
        ''',
        'coins_stats_by_gmtime_and_year': '''
            SELECT COUNT(coins.f),
                    SUM(coins.f),
                    AVG(coins.f),
                    MIN(coins.f),
                    MAX(coins.f)
                FROM {{bucket}}
                WHERE gmtime = {gmtime} and year = {year}
                GROUP BY gmtime, year
                LIMIT 20
        ''',
        'coins_stats_by_full_state_and_year': '''
            SELECT COUNT(coins.f),
                    SUM(coins.f),
                    AVG(coins.f),
                    MIN(coins.f),
                    MAX(coins.f)
                FROM {{bucket}}
                WHERE full_state.f = "{full_state[f]}" and year = {year}
                GROUP BY full_state.f, year
                LIMIT 20
        ''',
    }

    def __init__(self, index_type):
        self.view_sequence = cycle(self.VIEWS_PER_TYPE[index_type])

    def generate_query(self, bucket, view_name, **doc):
        return

    def next(self, doc):
        view_name = self.view_sequence.next()
        query = self.QUERIES[view_name].format(**doc)
        return None, None, query
