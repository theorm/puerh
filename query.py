# -*- coding: utf-8 -*-


if __name__ == '__main__':
    from puerh.elasticsearch import Query
    from datetime import datetime, timedelta

    start = datetime.now() - timedelta(days=3)
    end = datetime.now() - timedelta(days=2)

    query = Query()

    venues = [
        '4S-51e601e51b14527bb6b35530'
    ]

    print query.total('post', end=end, venues=venues)
