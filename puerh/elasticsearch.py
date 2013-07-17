# -*- coding: utf-8 -*-
from pyelasticsearch.client import ElasticSearch, JsonEncoder
from pyelasticsearch.exceptions import ElasticHttpNotFoundError
from bson import ObjectId


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
                'venue': {'type': 'string', 'index': 'not_analyzed'},
                'poster': {'type': 'string', 'index': 'not_analyzed'},
                'delta': {'type': 'long'}
            }
        }

        self._es.put_mapping(self._index, 'post', {'post': mapping})

    def add(self, event):

        data = {
            'timestamp': event['timestamp'],
            'venue': '{}-{}'.format(event['_id']['source'], event['venue']),
            'poster': '{}-{}'.format(event['_id']['source'], event['poster']),
            'delta': event.get('delta', 1)
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

    def total(self, event_type, start=None, end=None, venues=[]):

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

        terms = {}
        if venues:
            terms['venue'] = venues

        # 2. any venue/poster constraints?
        if terms:
            terms['execution'] = 'or'

            filters.append({
                'terms': terms,
            })

        query = {
            'filtered': {
                'query': {'match_all': {}},
                'filter': {
                    'and': filters
                }
            }
        }


        result = self._es.count(query, index=self._index)
        return result['count']

