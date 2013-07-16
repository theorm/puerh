# -*- coding: utf-8 -*-
import random
from bson import ObjectId
from datetime import datetime, timedelta


DEFAULT_PREFIXES = ('FB', 'IG', '4S')


def random_ids(n, prefixes=[]):
    '''Generate `n` random IDs
    '''
    for i in xrange(n):
        yield ObjectId()


def generate_post_events(start, end, max_step, num_venues, num_posters, prefixes=DEFAULT_PREFIXES):
    '''Generate a number of random post events.
    Parameters:
        `start` - minimum start datetime of the earliest event
        `end` - maximum end datetime of the latest event
        `max_step` - maximum timedelta step between events
        `num_venues` - number of random venues to use
        `num_posters` - number of random posters to use
        `prefixes` - prefixes to use for posters and venues IDs. makes number of venues and posters * len(prefixes)
    '''
    assert num_venues > 0
    assert num_posters > 0
    assert end > start
    assert max_step.total_seconds() > 0

    venues = {prefix:map(lambda id: '{}-{}'.format(prefix, id), random_ids(num_venues)) for prefix in prefixes}
    posters = {prefix:map(lambda id: '{}-{}'.format(prefix, id), random_ids(num_posters)) for prefix in prefixes}

    timestamp = start

    while timestamp < end:
        prefix = random.choice(prefixes)

        event = {
            '_id': '{}-{}'.format(prefix, ObjectId()),
            'type': 'POST',
            'timestamp': timestamp,
            'venue': random.choice(venues[prefix]),
            'poster': random.choice(posters[prefix]),
            'delta': 1
        }

        yield event

        timestamp += timedelta(seconds=random.randint(1, max_step.total_seconds()))

