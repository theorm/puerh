# -*- coding: utf-8 -*-

SOURCES = (
    ('facebook', 'FB'),
    ('instagram', 'IG'),
    ('foursquare', '4S')
)

PREFIXES = tuple([prefix for _, prefix in SOURCES])

_PREFIX_TO_SOURCE = {prefix:name for name, prefix in SOURCES}
_SOURCE_TO_PREFIX = {name:prefix for name, prefix in SOURCES}

def prefix_for_source(source):
    return _SOURCE_TO_PREFIX[source.lower()]

def source_for_prefix(prefix):
    return _PREFIX_TO_SOURCE[prefix.upper()]

def prefixed_id(source, id):
    prefix = prefix_for_source(source)
    return u'{}-{}'.format(prefix, id)
