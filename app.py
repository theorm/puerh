# -*- coding: utf-8 -*-


if __name__ == '__main__':
    import pymongo
    from puerh.generator import generate_post_events
    from puerh.elasticsearch import Indexer
    from datetime import datetime, timedelta


    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    step = timedelta(hours=1)
    num_venues = num_posters = 5

    conn = pymongo.MongoClient()
    coll = conn['test']['post_events']
    coll.drop()

    indexer = Indexer()

    indexer.cleanup()

    for e in generate_post_events(start, end, step, num_venues, num_posters):
        coll.insert(e)
        indexer.add(e)
