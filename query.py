# -*- coding: utf-8 -*-


if __name__ == '__main__':
    from puerh.elasticsearch import Query
    from datetime import datetime, timedelta

    start = datetime.now() - timedelta(days=365)
    end = datetime.now() - timedelta(days=20)

    query = Query()

    venues = [
    ]

    posters = [
    ]

    print query.total('post', start=start, end=end, venues=venues, posters=posters)

    print query.histogram('post', '7d', start=start, end=end, venues=venues, posters=posters, sources_facets=[])

    print query.top_terms('post', 'poster', start=start, end=end, venues=venues, posters=posters)
