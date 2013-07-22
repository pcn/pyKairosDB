# -*- python -*-

import time
from . import util
from util import tree
from . import metadata
from . import reader
from collections import deque


RETENTION_TAG = "storage-schema-retentions"
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR   = SECONDS_PER_MINUTE * 60
SECONDS_PER_DAY    = SECONDS_PER_HOUR   * 24
SECONDS_PER_WEEK   = SECONDS_PER_DAY    * 7
SECONDS_PER_MONTH  = SECONDS_PER_DAY    * 30 # OK, I'm approximating
SECONDS_PER_YEAR   = SECONDS_PER_DAY    * 365

# how graphite will access kairosdb
def graphite_metric_list_to_kairosdb_list(metric_list, tags):
    """
    :type metric_list: list
    :param metric_list: A list of lists/tuples, each one being the standard graphite formatting of metrics

    :type tags: dict
    :param tags: a dict of name: value pairs that will be recorded as tags

    :rtype: list
    :return: List of dicts formatted appropriately for kairosdb
    """
    return [graphite_metric_to_kairosdb(m, tags=tags) for m in metric_list]


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

    name_list = [ u'{0}'.format(n) for n in name.split(".")]
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
    expanded_name_list = util.metric_name_wildcard_expansion(cache_tree, name_list)
    return [ u".".join(en) for en in expanded_name_list]

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
    """
    :type metric: tuple
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

    """
    return {
        "name"      : metric[0],
        "timestamp" : metric[1],
        "value"     : metric[2],
        "tags"      : tags
    }

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
    def seconds_from_retention_tag(tag_value):
        """
        :type tag_value: str
        :param tag_value: the retention info tag

        :rtype: int
        :return: Number of seconds for the given tag value

        A tag is a colon-separated resolution:retention period.
        We're not worried about the retention period, we just care
        about the resolution of the data.

        So get the first part of it, and expand the number of seconds
        so we can make a valid comparison.
        """
        resolution, _ = tag_value.split(":")
        # It'd be nice to have a case statement here
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

    values = util.get_content_values_by_name(data, name)
    all_tags_set = set() # easiest case - all tags are the same, otherwise we use the set
    for result in values:
        all_tags_set.update(util.get_matching_tags_from_result(result, RETENTION_TAG))

    return max([ seconds_from_retention_tag(tag) for tag in all_tags_set])# return the lowest resolution

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
    content = conn.read_absolute([metric_name], start_time, end_time, query_modifying_function=cache_query())
    interval_seconds = _lowest_resolution_retention(content, metric_name)
    def modify_query():
        def modify_query_closure(query_dict):
            group_by               = reader.default_group_by()
            group_by["range_size"] = { "value" : interval_seconds, "unit" : "seconds"}
            aggregator = reader.default_aggregator()
            aggregator["sampling"] = group_by["range_size"]
            reader.group_by([group_by], query_dict["metrics"][0])
            reader.aggregation([aggregator], query_dict["metrics"][0])
        return modify_query_closure
    # re-fetch now that we've gotten the content and set the retention time.
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
