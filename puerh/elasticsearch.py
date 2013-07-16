# -*- coding: utf-8 -*-
from pyelasticsearch import ElasticSearch
from pyelasticsearch.exceptions import ElasticHttpNotFoundError

class Indexer(object):
    def __init__(self, url='http://localhost:9200/', index='events'):
        self._es = ElasticSearch(url)
        self._index = index

    def cleanup(self):
        try:
            self._es.delete_index(self._index)
        except ElasticHttpNotFoundError:
            pass
        self._es.create_index(self._index, settings={
            'index': {
                'mapper': {
                    'dynamic': True
                }
            }
        })

    def add(self, event):
        self._es.index(
            self._index, 
            event.get('type').lower(),
            event,
            id=event['_id'],
        )

