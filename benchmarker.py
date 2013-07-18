# -*- coding: utf-8 -*-
import pymongo
from puerh.elasticsearch import Indexer, Query
from puerh.generator import generate_post_events
from datetime import datetime, timedelta
import numpy

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

    def benchmark(self, name, start_window_days, end_window_days, 
        step_days, max_venues_filter):

        method = getattr(self, '_benchmark_{}'.format(name))
        
        assert start_window_days < end_window_days

        random_venues = self.get_random_venues(max_venues_filter)

        end_time = datetime.now()

        # an array to record time taken (without and with overhead)
        # per number of venues in the filter
        times = [([], []) for i in xrange(0, max_venues_filter)]
        # same with totals
        totals = [[0, 0] for i in xrange(0, max_venues_filter)]

        # increase number of venues in the filter by one for every iteration
        for venues_count in xrange(0, max_venues_filter):
            venues = random_venues[0:venues_count]

            # start with minimum days window
            days = start_window_days
            while days <= end_window_days:

                start_time = end_time - timedelta(days=days)

                with timer:
                    method(start_time, end_time, venues)

                took_without_overhead = self.elasticsearch_query.last_request_took()
                took_with_overhead = timer.duration_in_seconds()*1000

                totals[venues_count][0] += took_without_overhead
                totals[venues_count][1] += took_with_overhead
                times[venues_count][0].append(took_without_overhead)
                times[venues_count][1].append(took_with_overhead)

                print('{} days window with {} venues took elasticsearch {} ms ({} ms.)'.format(
                    days, venues_count, took_without_overhead, took_with_overhead))

                days += 1

        return [
            {
                'without_overhead': {
                    'total': t[0]/1000,
                    'min': numpy.min(s[0]),
                    'max': numpy.max(s[0]),
                    'median': numpy.median(s[0])
                },
                'with_overhead': {
                    'total': t[1]/1000,
                    'min': numpy.min(s[1]),
                    'max': numpy.max(s[1]),
                    'median': numpy.median(s[1])
                }
            }
            for t, s in zip(totals, times)
        ]

    def _benchmark_total_posts(self, start_time, end_time, venues):
        return self.elasticsearch_query.total('post', start=start_time, 
            end=end_time, venues=venues)

    def _benchmark_histogram_day(self, start_time, end_time, venues):
        return self.elasticsearch_query.histogram('post', 'day', start=start_time,
            end=end_time, venues=venues)

    def _benchmark_histogram_week(self, start_time, end_time, venues):
        return self.elasticsearch_query.histogram('post', 'week', start=start_time,
            end=end_time, venues=venues)

    def _benchmark_histogram_month(self, start_time, end_time, venues):
        return self.elasticsearch_query.histogram('post', 'month', start=start_time,
            end=end_time, venues=venues)

    def _benchmark_top_posters(self, start_time, end_time, venues):
        return self.elasticsearch_query.top_terms('post', 'poster', start=start_time,
            end=end_time, venues=venues)


if __name__ == '__main__':
    import sys
    benchmarker = Benchmarker()

    if 'generate' in sys.argv:
        days = 365 * 2
        delta = timedelta(minutes=2)
        venues = posters = 15

        print('Generating events...')
        generate_results = benchmarker.generate_events(days, delta, venues, posters)
        print('Generated events: {}'.format(generate_results))

        sys.exit(0)

    min_days = 1
    max_days = 500
    step_days = 1
    venues_count = 5

    benchmark_name = 'total_posts'

    t = Timer()

    with t:

        print('Benchmarking {} with: {} - {} days ({} day step) and {} max venues'.format(
            benchmark_name, min_days, max_days, step_days, venues_count))
        result = benchmarker.benchmark(benchmark_name, min_days, max_days, step_days, venues_count)

    for filter_venues, results in enumerate(result):
        print('{} with {} venues:'.format(benchmark_name, filter_venues))
        for result_type, data in results.iteritems():
            print('{}: {}'.format(result_type, data))
