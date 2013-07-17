# -*- coding: utf-8 -*-
from pyelasticsearch.client import ElasticSearch, JsonEncoder
from pyelasticsearch.exceptions import ElasticHttpNotFoundError
from bson import ObjectId
import random

class ESJSONEncoder(JsonEncoder):

    def default(self, value):
        if isinstance(value, ObjectId):
            return str(value)
        return super(ESJSONEncoder, self).default(value)

class Indexer(object):
    def __init__(self, url='http://localhost:9200/', index='events'):
        self._es = ElasticSearch(url)
        self._es.json_encoder = ESJSONEncoder
        self._index = index

    def cleanup(self):
        try:
            self._es.delete_index(self._index)
        except ElasticHttpNotFoundError:
            pass
        self._es.create_index(self._index, settings={
            'index': {
                'mapper': {
                    'dynamic': False
                }
            }
        })

        mapping = {
            'properties': {
                'timestamp': {'type': 'date', 'format': 'dateOptionalTime'},
                'source': {'type': 'string', 'index': 'not_analyzed'},
                'venue': {'type': 'string', 'index': 'not_analyzed'},
                'poster': {'type': 'string', 'index': 'not_analyzed'},
                'delta': {'type': 'integer'}
            }
        }

        self._es.put_mapping(self._index, 'post', {'post': mapping})

    def add(self, event):

        data = {
            'timestamp': event['timestamp'],
            'source': event['_id']['source'],
            'venue': '{}-{}'.format(event['_id']['source'], event['venue']),
            'poster': '{}-{}'.format(event['_id']['source'], event['poster']),
            'delta': random.choice((1,2,3)) #event.get('delta', 1)
        }

        self._es.index(
            self._index,
            event.get('type').lower(),
            data,
            id='{source}-{id}'.format(**event['_id'])
        )


class Query(object):
    def __init__(self, url='http://localhost:9200/', index='events'):
        self._es = ElasticSearch(url)
        self._index = index

    def total(self, event_type, start=None, end=None, venues=[], posters=[]):
        '''Returns event's sum of deltas broken down per source:
            {
                'IG': 25,
                'FB': 3,
                ...
            }

            Can be filtered by start and end dates, venues or posters.
        '''

        filters = []

        timestamp_range = {}
        if start:
            timestamp_range['gte'] = start
        if end:
            timestamp_range['lte'] = end

        # 1. if timestamp range is provided - add it
        if timestamp_range:
            filters.append({
                'range': {'timestamp': timestamp_range}
            })

        if venues:
            terms = {
                'venue': venues,
                'execution': 'or'
            }
            filters.append({
                'terms': terms,
            })

        if posters:
            terms = {
                'poster': posters,
                'execution': 'or'
            }
            filters.append({
                'terms': terms,
            })


        query = {
            'query': {'match_all': {}},
            'filter': {
            },
            'facets': {
                'events_deltas_totals': {
                    'terms_stats': {
                        'key_field': 'source',
                        'value_field': 'delta'
                    },
                    'facet_filter': {
                        'and': filters
                    }
                }
            }
        }


        result = self._es._search_or_count(
            '_search',
            query, 
            index=self._index, 
            query_params={'search_type': 'count'}
        )
        facets = result['facets']['events_deltas_totals']['terms']
        return {
            f['term']: f['total']
            for f in facets
        }
