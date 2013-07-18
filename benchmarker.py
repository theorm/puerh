# -*- coding: utf-8 -*-
import pymongo
from puerh.elasticsearch import Indexer, Query
from puerh.generator import generate_post_events
from datetime import datetime, timedelta

import random
import time
 
class Timer(object):
    def __enter__(self):
        self.__start = time.time()
 
    def __exit__(self, type, value, traceback):
        # Error handling here
        self.__finish = time.time()
 
    def duration_in_seconds(self):
        return self.__finish - self.__start
 
timer = Timer()

class Benchmarker(object):

    def __init__(self, mongo_url='mongodb://localhost:27017', db_name='test', 
        collection_name='events', elasticsearch_url='http://localhost:9200'):

        mongo_client = pymongo.MongoClient(mongo_url)
        self.mongo_events_collection = mongo_client[db_name][collection_name]

        self.elasticsearch_indexer = Indexer(elasticsearch_url, 
            index=collection_name)

        self.elasticsearch_query = Query(elasticsearch_url, 
            index=collection_name)


    def generate_events(self, days_in_history, max_time_step, total_random_venues, 
        total_random_posters):
        assert isinstance(max_time_step, timedelta)

        start = datetime.now() - timedelta(days=days_in_history)
        end = datetime.now()

        events_count = 0

        # remove old data
        self.mongo_events_collection.drop()
        self.elasticsearch_indexer.cleanup()

        for e in generate_post_events(start, end, max_time_step, 
            total_random_venues, total_random_posters):
            self.mongo_events_collection.insert(e)
            self.elasticsearch_indexer.add(e)
            events_count += 1

        return {
            'earliest event time': start,
            'latest event time': end,
            'maximum time step': max_time_step,
            'events generated': events_count
        }

    def get_random_venues(self, count):
        sources = self.mongo_events_collection.distinct('_id.source')
        return [
            '{}-{}'.format(random.choice(sources), i) 
            for i in self.mongo_events_collection.distinct('venue')
        ]


    def benchmark_total_posts(self, start_window_days, end_window_days, 
        step_days, max_venues_filter):
        assert start_window_days < end_window_days

        random_venues = self.get_random_venues(max_venues_filter)

        end_time = datetime.now()


        days = start_window_days
        while days <= end_window_days:

            start_time = end_time - timedelta(days=days)

            for venues_count in xrange(0, max_venues_filter):
                venues = random_venues[0:venues_count]

                with timer:
                    result = self.elasticsearch_query.total('post', start=start_time,
                        end=end_time, venues=venues)

                print('{} days window with {} venues took elasticsearch {}ms: {}'.format(
                    days, venues_count, timer.duration_in_seconds()*1000, result))

            days += 1

    def benchmark_histogram(self, start_window_days, end_window_days, 
        step_days, max_venues_filter):

        assert start_window_days < end_window_days

        random_venues = self.get_random_venues(max_venues_filter)

        end_time = datetime.now()


        days = start_window_days
        while days <= end_window_days:

            start_time = end_time - timedelta(days=days)

            for venues_count in xrange(0, max_venues_filter):
                venues = random_venues[0:venues_count]

                with timer:
                    result = self.elasticsearch_query.histogram('post', 'month', start=start_time,
                        end=end_time, venues=venues)

                print('{} days window with {} venues took elasticsearch {}ms: {}'.format(
                    days, venues_count, timer.duration_in_seconds()*1000, len(result)))

            days += 1

    def benchmark_top_posters(self, start_window_days, end_window_days, 
        step_days, max_venues_filter):

        assert start_window_days < end_window_days

        random_venues = self.get_random_venues(max_venues_filter)

        end_time = datetime.now()


        days = start_window_days
        while days <= end_window_days:

            start_time = end_time - timedelta(days=days)

            for venues_count in xrange(0, max_venues_filter):
                venues = random_venues[0:venues_count]

                with timer:
                    result = self.elasticsearch_query.top_terms('post', 'poster', start=start_time,
                        end=end_time, venues=venues)

                print('{} days window with {} venues took elasticsearch {}ms: {}'.format(
                    days, venues_count, timer.duration_in_seconds()*1000, len(result)))

            days += 1

if __name__ == '__main__':
    benchmarker = Benchmarker()

    generate_results = benchmarker.generate_events(365 * 4, timedelta(minutes=2), 15, 15)
    print('Generated events: {}'.format(generate_results))

    # print('Benchmarking total posts...')
    # benchmarker.benchmark_total_posts(1, 500, 1, 5)

    # print('Benchmarking histogram...')
    # benchmarker.benchmark_histogram(1, 500, 1, 5)

    # print('Benchmarking top posters...')
    # benchmarker.benchmark_top_posters(1, 500, 1, 5)
