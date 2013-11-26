# -*- python -*-

import time
from . import util
from util import tree
from . import metadata
from . import reader
from collections import deque
import fnmatch
import re


RETENTION_TAG = "gr-ret" # terse in order to save space on-disk
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR   = SECONDS_PER_MINUTE * 60
SECONDS_PER_DAY    = SECONDS_PER_HOUR   * 24
SECONDS_PER_WEEK   = SECONDS_PER_DAY    * 7
SECONDS_PER_MONTH  = SECONDS_PER_DAY    * 30 # OK, I'm approximating
SECONDS_PER_YEAR   = SECONDS_PER_DAY    * 365

INVALID_CHARS      = re.compile(r'[^A-Za-z0-9-_/.]')
RET_SEPERATOR_CHAR = "_" # Character we use to separate the retentions
RET_GRAPHITE_CHAR  = ":" # Character graphite uses to separate the retentions

def _graphite_metric_list_retentions(metric_list, storage_schemas):
    """:type metric_list: list
    :param metric_list: a list of lists/tuples, each one is the standard graphite format of metrics

    :type storage_schemas: list
    :param storage_schemas: list of carbon.stroage.Schema objects which will be matched against
                            each of the graphite metrics and used to tag each metric.

    """
    def get_retentions(metric_name):
        for s in storage_schemas:
            if s.test(metric_name):
                return _input_retention_resolution(s.options['retentions'].split(','))
    retentions = [ get_retentions(m[0])[1].replace(RET_GRAPHITE_CHAR, RET_SEPERATOR_CHAR) for m in metric_list ]
    return retentions

# how graphite will access kairosdb
def graphite_metric_list_to_kairosdb_list(metric_list, tags):
    """
    :type metric_list: list
    :param metric_list: A list of lists/tuples, each one being the standard graphite formatting of metrics

    :type tags: dict
    :param tags: a dict of name: value pairs that will be recorded as tags

    :rtype: list
    :return: List of dicts formatted appropriately for kairosdb

    Use this for entering a large set of metrics that have the same set of tags (e.g. same retentions, etc.)
    """
    return [graphite_metric_to_kairosdb(m, tags=tags) for m in metric_list]

# how graphite will write lots of points to kairosdb
def graphite_metric_list_with_retentions_to_kairosdb_list(metric_list, storage_schemas, pervasive_tags={}):
    """
    :type metric_list: list
    :param metric_list: A list of line-formatted lists or tuples, each one being the standard graphite formatting of metrics

    :type storage_schemas: list
    :param storage_schemas: A list of carbon.storage.Schema objects to be matched against.

    :type pervasive_tags: dict
    :param pervasive_tags: tags that will be applied to (almost) all metrics. This won't override the
                           retentions configuration.

    :rtype: generator
    :return: generator of dicts formatted appropriately for kairosdb

    Use this for entering a large set of metrics that have the disparate retentions.  The expected
    way to call this from a sender is to first call _graphite_metric_list_retentions()

    XXX this API is getting messy - it should be simpler. -PN
    """
    retentions_list = _graphite_metric_list_retentions(metric_list, storage_schemas)
    for m,r in zip(metric_list, retentions_list):
        tags = {}
        if len(pervasive_tags) > 0:
            tags.update(pervasive_tags)
        tags[RETENTION_TAG] = r
        yield graphite_metric_to_kairosdb(m, tags=tags)


def _fnmatch_expand_graphite_wildcard_metric_name(conn, name, cache_ttl=60):
    """
    :type conn: pyKairosDB.KairosDBConnection
    :param conn: the connection to the database

    :type name: string
    :param name: the graphite-like name which can include ".*." to provide wildcard expansion

    :type cache_ttl: int
    :param cache_ttl: how often to update the cache from KairosDB, in seconds

    :rtype: list
    :return: a list of unicode strings.  Each unicode string contains an expanded metric name.

    KairosDB doesn't currently support wildcards, so get all metric
    names and expand them.

    Currently only ".*." or "\*." or ".\*" expansions are supported.
    Substring expansions aren't supported at this time.

    Graphite-web uses fnmatch or something similar, perhaps this
    should provide a list and re-use the same functionality.

    This function caches the created tree for cache_ttl seconds and
    refreshes when the cache has aged beyond the cache_ttl.
    """
    all_metric_name_list = metadata.get_all_metric_names(conn)
    return [ n for n in all_metric_name_list if fnmatch.fnmatch(n, name) ]

def expand_graphite_wildcard_metric_name(conn, name, cache_ttl=60):
    """
    :type conn: pyKairosDB.KairosDBConnection
    :param conn: the connection to the database

    :type name: string
    :param name: the graphite-like name which can include ".*." to provide wildcard expansion

    :type cache_ttl: int
    :param cache_ttl: how often to update the cache from KairosDB, in seconds

    :rtype: list
    :return: a list of unicode strings.  Each unicode string contains an expanded metric name.

    KairosDB doesn't currently support wildcards, so get all metric
    names and expand them.

    Currently only ".*." or "\*." or ".\*" expansions are supported.
    Substring expansions aren't supported at this time.

    Graphite-web uses fnmatch or something similar, perhaps this
    should provide a list and re-use the same functionality.

    This function caches the created tree for cache_ttl seconds and
    refreshes when the cache has aged beyond the cache_ttl.
    """

    if "*" not in name:
        return [u'{0}'.format(name)]

    if "." in name:
        name_list = [ u'{0}'.format(n) for n in name.split(".")]
    else:
        name_list = [ name ]
    # print "Name_list is {0}".format(name_list)

    ts        = expand_graphite_wildcard_metric_name.cache_timestamp
    cache_tree = expand_graphite_wildcard_metric_name.cache_tree
    if ts == 0 or (time.time() - ts > cache_ttl):
        all_metric_name_list = metadata.get_all_metric_names(conn)
        cache_tree           = tree()
        _make_graphite_name_cache(cache_tree, all_metric_name_list)
        expand_graphite_wildcard_metric_name.cache_tree      = cache_tree
        expand_graphite_wildcard_metric_name.cache_timestamp = time.time()
    if name == "*": # special case for the root of the tree:
        return cache_tree.keys()
    if '*' in name and not '.' in name:
        return [ ctk for ctk in cache_tree.keys() if fnmatch.fnmatch(ctk, name)]
    expanded_name_list = util.metric_name_wildcard_expansion(cache_tree, name_list)
    # print "expanded_name_list is {0}".format(expanded_name_list)

    return_list = [ ".".join(en) for en in expanded_name_list]
    return list(set(return_list))

expand_graphite_wildcard_metric_name.cache_tree = tree()
expand_graphite_wildcard_metric_name.cache_timestamp = 0


def leaf_or_branch(conn, name):
    """
    :type conn: pyKairosDB.KairosDBConnection
    :param conn: Connection to the pyrosdb

    :type name: string
    :param name: The metric name or part of a name that we're checking for

    :rtype: str
    :return: Either the string "leaf" or "branch"

    Graphite wants to know if a name is a "leaf" or a "branch" in
    its ultimate storage location that is, whether or not it can be
    traversed further
    """
    # print "Trying to expand the name {0}".format(name)
    if name.endswith('*'):
        wildcard_expansion = expand_graphite_wildcard_metric_name(conn, name)
    else:
        wildcard_expansion = expand_graphite_wildcard_metric_name(conn, name + ".*")

    if len(wildcard_expansion) > 0:
        return "branch"
    else:
        return "leaf"


def _make_graphite_name_cache(cache_tree, list_of_names):
    """    :type cache_tree: defaultdict
    :param cache_tree: a defaultdict initialized with the tree() function.  Contains names
        of entries in the kairosdb, separated by "." per the graphite convention.

    :type list_of_names: list
    :param list_of_names: list of strings, in order, that will be sought after in the cache tree.

    Given a list of names - all name - that kairosdb has, make a
    tree of all those names.
    """
    for n in list_of_names:
        util._add_to_cache(cache_tree, n.split('.'))

def graphite_metric_to_kairosdb(metric, tags):
    """:type metric: tuple
    :param metric: tuple of ("metric_name", timestamp, value)

    :type tags: dict
    :param tags: a dict of {name: value} strings that will be recorded as tags

    :rtype: dict
    :return: Re-formatted dict appropriate for kairosdb

    Write graphite metrics to KairosDB.

    Graphite metrics are a tuple with a metric name, a timestamp, and
    a value, and they have a storage schema attached, which specifies
    the time period which should be used for that metric.  This must
    be recorded in the tags for graphite querying to work

    KairosDB metrics are a hash of
    {
     "name"      : string,
     "timestamp" : java long int,
     "value"     : float,
     "tags"      : { "name" : "value", "name" : "value"}
    }


    Even though KairosDB uses a long int, the pythong API here expects
    a float, as returned by time.time().  This module handles
    converting this when the data is written and read, and doesn't
    make the user deal with this conversion.

    KairosDB only allows alphanumeric and the following punctuation characters:

    ".", "/", "-", and "_".

    Graphite is less restrictive.  Anything that doesn't match the
    above are converted to an underscore.

    """
    converted_metric_name = INVALID_CHARS.sub(TAG_SEPERATOR_CHAR, metric[0])
    return {
        "name"      : converted_metric_name,
        "timestamp" : metric[1],
        "value"     : metric[2],
        "tags"      : tags
    }


def seconds_from_retention_tag(tag_value, sep=RET_GRAPHITE_CHAR):
    """:type tag_value: str
    :param tag_value: the retention info tag

    :rtype: int
    :return: Number of seconds for the given tag value

    The retention tag is a colon-separated resolution_retention period
    when it's input, taken from the carbon storage-schemas.conf.  When
    reading from kairosdb, the ':' is not a legal character to have in
    a tag, so we input them with an underscore instead.  That
    separator is configurable so this can be used on tags that are
    queried as well.

    For this function we're not worried about the retention period, we
    just care about the resolution of the data.

    So get the first part of it, and expand the number of seconds
    so we can make a valid comparison.

    """
    resolution, _ = tag_value.split(sep)
    if resolution[-1].lower() == "s":
        return int(resolution[:-1])
    elif resolution[-1].lower() == "m":
        return int(resolution[:-1]) * SECONDS_PER_MINUTE
    elif resolution[-1].lower() == "h":
        return int(resolution[:-1]) * SECONDS_PER_HOUR
    elif resolution[-1].lower() == "d":
        return int(resolution[:-1]) * SECONDS_PER_DAY
    elif resolution[-1].lower() == "w":
        return int(resolution[:-1]) * SECONDS_PER_WEEK
    elif resolution[-1].lower() == "y":
        return int(resolution[:-1]) * SECONDS_PER_YEAR
    else: # Seconds is the default
        return int(resolution)

def _input_retention_resolution(retention_string_list):
    """
    :type retention_string_list: list
    :param retention_string_list: A list of strings containing retention definitions

    :rtype: tuple
    :return: A tuple containing the (number_of_seconds, highest_resoultion_retention_string),
             which is an (int, str)

    When inputting data, we want the highest resolution data - the
    actual metrics can be summarized later based on the same policy or
    a different one if that works better.
    """
    all_tags_set = [t for t in retention_string_list]
    return min([(seconds_from_retention_tag(tag), tag) for tag in all_tags_set])# return the highest resolution


def _lowest_resolution_retention(data, name):
    """
    :type data: requests.response.content
    :param data: Content from the KairosDB query

    :type name: str
    :param name: The name of the metric whose retention will be extracted.

    :rtype: int
    :return: The number of seconds in the lowest-resolution retention period in the data

    Graphite needs data to be divided into even time slices.  We
    must store the slice information when writing data so that it can
    be read out here.

    The relevant tags are in the README.md for this project.
    """
    values = util.get_content_values_by_name(data, name)
    all_tags_set = set() # easiest case - all tags are the same, otherwise we use the set
    for result in values:
        all_tags_set.update(util.get_matching_tags_from_result(result, RETENTION_TAG))
    return max([ seconds_from_retention_tag(tag, RET_SEPERATOR_CHAR) for tag in all_tags_set])# return the lowest resolution

def read_absolute(conn, metric_name, start_time, end_time):
    """
    :type conn: pyKairosDB.KairosDBConnection
    :param conn: The connection to KairosDB

    :type metric_name: string
    :param metric_name: The name of the metric to query (graphite does one at a time, though KairosDB can do more)

    :type start_time: float
    :param start_time: The float representing the number of seconds since the epoch that this query starts at.

    :type end_time: float
    :param end_time: The float representing the number of seconds since the epoch that this query endsa at.

    :rtype: tuple
    :return: 2-element tuple - ((start_time, end_time, interval), list_of_metric_values).  Graphite wants evenly-spaced metrics,
        and None for any interval that doesn't have data.  It infers the time for each update by the order and place of each
        value provided.

    This function returns the values being queried, in the format that the graphite-web app requires.
    """
    def cache_query():
        def cache_query_closure(query_dict):
            reader.cache_time(10, query_dict)
        return cache_query_closure
    tags = conn.read_absolute([metric_name], start_time, end_time,
                              query_modifying_function=cache_query(),
                              only_read_tags=True)

    interval_seconds = _lowest_resolution_retention(tags, metric_name)
    def modify_query():
        def modify_query_closure(query_dict):
            group_by               = reader.default_group_by()
            group_by["range_size"] = { "value" : interval_seconds, "unit" : "seconds"}
            aggregator = reader.default_aggregator()
            aggregator["sampling"] = group_by["range_size"]
            reader.group_by([group_by], query_dict["metrics"][0])
            reader.aggregation([aggregator], query_dict["metrics"][0])
        return modify_query_closure
    # now that we've gotten the tags and have set the retention time, get data
    content = conn.read_absolute([metric_name], start_time, end_time,
        query_modifying_function=modify_query())
    return_list = list()
    if len(content['queries'][0]['results']) > 0:
        # by_interval_dict = dict([(v[1], v[0]) for v in content["queries"][0]["results"][0]["values"] ])
        value_deque = deque(content["queries"][0]["results"][0]["values"])
        slots = list()
        for slot_begin in range(start_time, end_time, interval_seconds):
            slot_buffer = list()
            slot_end = slot_begin + interval_seconds
            slots.append((slot_begin, slot_end))
            try:
                if slot_end < value_deque[0][0]: # we haven't caught up with the beginning of the deque
                    return_list.append(None)
                    continue
                if slot_begin > value_deque[-1][0]: # We have nothing more of value
                    return_list.append(None)
                    continue
                if len(value_deque) == 0:
                    return_list.append(None)
                    continue
                while slot_begin <= value_deque[0][0] < slot_end:
                    slot_buffer.append(value_deque.popleft()[1])
            except IndexError:
                return_list.append(None)
            if len(slot_buffer) < 1:
                return_list.append(None)
            else:
                return_list.append(sum(slot_buffer)/len(slot_buffer)) # take the average of the points for this slot
    else:
        return_list = [ None for n in range(start_time, end_time, interval_seconds)]
    return ((start_time, end_time, interval_seconds), return_list)
