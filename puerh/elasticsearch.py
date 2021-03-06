# -*- coding: utf-8 -*-
from pyelasticsearch.client import ElasticSearch, JsonEncoder
from pyelasticsearch.exceptions import ElasticHttpNotFoundError
from .source import PREFIXES
from bson import ObjectId
import copy
from datetime import datetime
from functools import partial

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

        not_analyzed_mapping = {
            'properties': {
                'timestamp': {'type': 'date', 'format': 'dateOptionalTime'},
                'source': {'type': 'string', 'index': 'not_analyzed'},
                'venue': {'type': 'string', 'index': 'not_analyzed'},
                'poster': {'type': 'string', 'index': 'not_analyzed'},
                'delta': {'type': 'integer'}
            }
        }

        analyzed_mapping = {
            'properties': {
                'timestamp': {'type': 'date', 'format': 'dateOptionalTime'},
                'source': {'type': 'string', 'analyzer': 'keyword'},
                'venue': {'type': 'string', 'analyzer': 'keyword'},
                'poster': {'type': 'string', 'analyzer': 'keyword'},
                'delta': {'type': 'integer'}
            }
        }

        hybrid_mapping = {
            'properties': {
                'timestamp': {'type': 'date', 'format': 'dateOptionalTime'},
                'source': {'type': 'string', 'analyzer': 'keyword'},
                'venue': {'type': 'string', 'analyzer': 'whitespace'},
                'poster': {'type': 'string', 'analyzer': 'whitespace'},
                'delta': {'type': 'integer'}
            }
        }


        mapping = not_analyzed_mapping

        self._es.put_mapping(self._index, 'post', {'post': mapping})

    def add(self, event):

        data = {
            'timestamp': event['timestamp'],
            'source': event['_id']['source'],
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


    def last_request_took(self):
        ''' A number of processing time in milliseconds
        reported by ElasticSearch without our client code overhead.
        '''
        return self._last_request_took

    def _build_filter(self, event_type, start=None, end=None, source=None, **kwargs):
        '''Build an 'AND' filter that combines filters:
            1. correct `event_type`
            2. more or equal than start time (if provided)
            3. less or equal than end time (if provided)
            4. filter by values of terms in kwargs
        '''


        filters = []

        # 0. event type
        filters.append({
            'term': {'_type': event_type}
        })

        if source is not None:
            filters.append({
                'term': {'source': source}
            })

        for term_name, term_values in kwargs.iteritems():
            if term_values:
                terms = {
                    'venue': term_values,
                    'execution': 'or'
                }
                filters.append({
                    # XXX see if this query speeds up things
                    # UPD: not really
                    # if mapping uses analyzed strings

                    # 'query': {
                    #     'query_string': {
                    #         'query': ' OR '.join(term_values)
                    #     }
                    # }
                    'terms': terms,
                })


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

        # BOOL filter is more performand than AND:
        # http://www.elasticsearch.org/blog/all-about-elasticsearch-filter-bitsets/
        # return {'and': filters}
        return {
            'bool': {
                'must': filters
            }
        }


    def total(self, event_type, start=None, end=None, venues=[], posters=[]):
        '''Returns event's sum of deltas broken down per source:
            {
                'IG': 25.0,
                'FB': 3.0,
                ...
            }

            Can be filtered by start and end dates, venues or posters.
        '''

        filters = self._build_filter(event_type, start=start, end=end, 
            venues=venues, posters=posters)

        query = {
            'facets': {
                'events_deltas_totals': {
                    'terms_stats': {
                        'key_field': 'source',
                        'value_field': 'delta'
                    },
                    'facet_filter': filters
                }
            }
        }


        result = self._es._search_or_count(
            '_search',
            query, 
            index=self._index, 
            query_params={'search_type': 'count'}
        )
        self._last_request_took = result['took']
        facets = result['facets']['events_deltas_totals']['terms']
        return {
            f['term']: f['total']
            for f in facets
        }

    def top_terms(self, event_type, term, limit=10, start=None, end=None, venues=[], posters=[]):
        '''Returns `limit` top terms with their count.
        `term` can be one of: `poster`, `venue`, `source`.

        This is a more flexible version of top posters.

        The rest of arguments do the same as in `total` function.
        '''

        assert term in ('poster', 'venue', 'source')

        filters = self._build_filter(event_type, start=start, 
            end=end, venues=venues, posters=posters)

        query = {
            'facets': {
                'top': {
                    'terms': {
                        'field': term,
                        'size': limit
                    },
                    'facet_filter': filters
                }
            }
        }

        result = self._es._search_or_count(
            '_search',
            query, 
            index=self._index, 
            query_params={'search_type': 'count'}
        )
        self._last_request_took = result['took']
        facets = result['facets']['top']['terms']
        return facets


    def _format_histogram_facet_values(self, values):
        return [
            {'time': datetime.utcfromtimestamp(v['time']/1000), 'total': v['total']}
            for v in values['entries']
        ]

    def histogram(self, event_type, interval, start=None, end=None, 
            venues=[], posters=[], sources_facets=PREFIXES, include_total=False):
        '''Returns histogram of events deltas totals in buckets by `interval` apart.

        {
            'total': [
                {'time': <datetime-1>, 'total': 3.0},
                {'time': <datetime-2>, 'total': 1.0},
            ],
            'FB': [
                ...
            ]
        }

        Filter parameters are the same as in `total` method.

        Source facets are taken from `sources_facets` param. If you don't want them,
        just pass an empty list.

        Total facet is included by default. If you don't need it, set 
        `include_total` to `False`.
        '''
        
        filter_builder = partial(self._build_filter, event_type, start=start, 
            end=end, venues=venues, posters=posters)


        date_histogram_value = {
            'key_field': 'timestamp',
            'value_field': 'delta',
            'interval': interval,
        }

        facets = {}

        if include_total:
            filters = filter_builder(source=None)
            facets['total'] = {
                'date_histogram': date_histogram_value,
                'facet_filter': filters
            }


        for source in sources_facets:
            filters = filter_builder(source=source)

            payload = {
                'date_histogram': date_histogram_value,
                'facet_filter': filters
            }
            facets[source] = payload

        result = self._es._search_or_count(
            '_search',
            query={'facets': facets}, 
            index=self._index, 
            query_params={'search_type': 'count'}
        )
        self._last_request_took = result['took']

        return {
            facet: self._format_histogram_facet_values(values) 
            for facet, values in result['facets'].iteritems()
        }
