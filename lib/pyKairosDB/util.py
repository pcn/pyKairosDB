# -*- python -*-

"""This module contains ease-of-use functions"""
import time

from collections import defaultdict
from . import metadata as metadata

# import macropy

# Nice hack: https://gist.github.com/hrldcpr/2012250
def tree():
  """This will create a tree of defaultdicts... of trees.  This is a
  nice hack, and auto-vivifies the necessary dicts.
  """
  return defaultdict(tree)

def get_content_values_by_name(content, name):
    """When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    Content will look like this when it comes back as json:
        "{\"queries\":
       [
         {\"results\":
           [
             {\"name\":\"test\",
              \"tags\":
                {\"graphite\":[\"yes\"]},
              \"values\":[
                [1373780448859,1.373780448859967E9],
                [1373780450816,1.373780450816504E9],
                [1373781047785,1.373781047785849E9],
                [1373781261751,1.373781261751651E9],
                [1373781264045,1.37378126404582E9],
                [1373781265456,1.373781265456353E9]
              ]
            }
          ]
        }
      ]
    }"

    and this will be turned into a python struct.  This function will
    return the dict that matches the reult whose name and matches the
    argument "name"

    """
    for q in content["queries"]:
        for r in q["results"]:
            if r["name"] == name:
                return r

def content_by_name_substring(content, name):
    """When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    Content will look like this when it comes back as json:

    and this will be turned into a python struct.  This function will
    return a list of dicts that matched the provided name.

    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type name: string
    :param name: This is the string that will be used to match, as a lowercase substring, the things that we want to return.

    """
    r_list = list()
    for q in content["queries"]:
        for r in q["results"]:
            if name.lower() in r["name"].lower():
                r_list.append(r)
    return r_list




def get_content_values_by_tag_info(content, tag_key, tag_value_list):
    """When you've got content, but only want to look at a piece of
    it, specifically the values that are provided which match one more
    tag key and some number of tag values, then use this function.

    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type tag_key: list
    :param tag_key: This will index the tag that we're interested in

    :type tag_value_list: list
    :param tag_value_list: list of strings, each of which is a possible tag

    :rtype: list
    :return: a list of result dicts that matched
    """
    r_list = list()
    tag_value_set = frozenset([tv.lower() for tv in tag_value_list])
    for q in content["queries"]:
        for r in q["results"]:
            for t in r["tags"]:
                if tag_key.lower() in [ each_tag.lower() for each_tag in t.keys() ]:
                    # This creates the irritating situation where the tag could have mis-matched
                    # casing - e.g. apple and Apple match by the comparison above.
                    content_tag_set = frozenset([ tv.lower() for tv in t.values])
                    if tag_value_set in content_tag_set:
                        r_list.append(r_list)
    return r_list

def graphite_metric_to_kairosdb(metric, tags={"graphite" : "graphite"}):
    """Graphite metrics are a tuple with a metric name, a timestamp, and a value.

    If tags are added to graphite, then that's cool.  We'll do that later.

    KairosDB metrics are a hash of
    {
     "name"      : string,
     "timestamp" : java long int,
     "value"     : float,
     "tags"      : { "name" : "value", "name" : "value"}
    }

    However, the value we supply will be a python float, and that'll
    be handled when the data is written.

    :type metric: tuple
    :param metric: tuple per the standard graphite formatting of metrics

    :type tags: dict
    :param tags: a dict of name: value pairs that will be recorded as tags

    :rtype: dict
    :return: Re-formatted dict appropriate for kairosdb
    """
    return {
        "name"      : metric[0],
        "timestamp" : metric[1],
        "value"     : metric[2],
        "tags"      : tags
    }

def graphite_metric_list_to_kairosdb_list(metric_list):
    """Doesn't handle tags yet
    :type metric_list: list
    :param metric_list: A list of lists/tuples, each one being the standard graphite formatting of metrics

    :type tags: dict
    :param tags: a dict of name: value pairs that will be recorded as tags

    :rtype: list
    :return: List of dicts formatted appropriately for kairosdb
    """
    return [graphite_metric_to_kairosdb(m) for m in metric_list]



def _add_to_cache(cache_tree, list_of_names):
    """This should probably be done via macropy, I need to understand
    more about how that'd work first.  I'd like to have macro expansion
    of "list_of_names", e.g. ['a', 'b', 'c'] to turn into
    cache_tree['a']['b']['c']

    Cache_tree is a tree of dictionaries, so it will be mutated in
    place.

    :type cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function.  Contains names
                             of entries in the kairosdb, separated by "." per the graphite convention.
                             This is modified in-place

    :type list_of_names: list
    :description list_of_names: list of strings, in order, that will be sought after in the cache tree.

    """
    head_item = list_of_names[0]
    cache_tree[head_item]
    if len(list_of_names) == 1:
        return
    else:
        tail_list = list_of_names[1:]
        _add_to_cache(cache_tree[head_item], tail_list)


def _match_in_cache(cache_tree, list_of_names):
    """Given a cache_tree, and a prefix, returns all of the values associated with that prefix,
    that is, the keys that reside under the prefix.
    :type cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function.  Contains names
                             of entries in the kairosdb, separated by "." per the graphite convention.

    :type list_of_names: list
    :description list_of_names: list of strings, in order, that will be sought after in the cache tree.

    :rtype: list
    :return: A list of matches, possibly empty.
    """
    head_item = list_of_names[0]
    if head_item not in cache_tree.keys():
        return [] # Empty List to signify we're done here
    elif len(list_of_names) == 1:
        return cache_tree[head_item].keys()
    else:
        tail_list = list_of_names[1:]
        return _match_in_cache(cache_tree[head_item], tail_list)


def _make_graphite_name_cache(cache_tree, list_of_names):
    """Given a list of names - all name - that kairosdb has, make a tree of all those names.

    :type cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function.  Contains names
                             of entries in the kairosdb, separated by "." per the graphite convention.

    :type list_of_names: list
    :description list_of_names: list of strings, in order, that will be sought after in the cache tree.
    """
    for n in list_of_names:
        _add_to_cache(cache_tree, n.split('.'))

# XXX cache_tree should be re-named because it's not a cache tree, exactly, it's a tree
# of all names
def metric_name_wildcard_expansion(cache_tree, name_list):
    """Given some metrics, break up the list around * entries and
    return all those that match.

    This only supports wildcards that are standalone - e.g. I don't
    know whether or not the web ui expects "foo.* and foo.a* to both
    work (the former I know works, the latter, well...?) but this will
    only deal with an * surrounded by a wildcard.

    This only returns the prefixes implied by the *.   E.g. for the list:
      ['a', 'b', '*']

    and the tree
    {
      'a' : {
        'b' : {
          '1' : {},
          'c' : {},
          'd' : {
            {
              'a': {},
              'b': {},
              'c': {}
            }
          }
        }
      }
    }

    applying the name_list tothe cache_tree will return
    [[['a', 'b', '1']], [['a', 'b', 'c']], [['a', 'b', 'd']]]


    :param cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function

    :param name_list: list
    :description name_list: a list of strings created by splitting a graphite name along "." boundaries.

    :rtype: list
    :return: a list of lists.  Each sub-list is the split-out metric names.
    """
    count = 0
    complete_list = list()
    for n in range(len(name_list)):
        if name_list[n] == '*':
            before_list = name_list[0:n]
            after_list = name_list[n+1:]
            # print "before_list is {0}, n is {1}".format(before_list, n)
            expansion_list = _match_in_cache(cache_tree, before_list)
            expanded_name_list_of_list = [before_list + [e] + after_list for e in expansion_list ]

            for expansion in expanded_name_list_of_list:
                expanded_maybe_empty = metric_name_wildcard_expansion(cache_tree, expansion)
                # print "Expanded_maybe_empty is {0}".format(expanded_maybe_empty)
                if len(expanded_maybe_empty) > 0:
                    if len(after_list) == 0: # if this is a terminal node in the tree, then append
                        complete_list.append(expanded_maybe_empty)
                    else: # otherwise, extend the complete list, this is already a list_of_lists
                        complete_list.extend(expanded_maybe_empty)
            # print "returning complete_list: {0}".format(complete_list)
            return complete_list # Any other stars will be expanded recursively.
    return name_list # There were no asterisks

def expand_graphite_wildcard_metric_name(conn, name, cache_ttl = 60):
    """KairosDB doesn't currently support wildcards, so get all metric
    names and expand them.

    Currently only ".*." or "*." or ".*" expansions are supported.
    Substring expansions aren't supported at this time.

    Cache the created tree for cache_ttl seconds and refresh when the cache has aged.

    :param conn: pyKairosDBConnection
    :description conn: the connection to the database

    :param name: string
    :description name: the graphite-like name which can include ".*." to provide wildcard expansion

    :param cache_ttl: int
    :description cache_ttl: how often to update from the cache in KairosDB, in seconds

    :rtype: list
    :return: a list of unicode strings.  Each unicode string contains an expanded metric name

    """
    if "*" not in name:
        return [u'{0}.'.format(name)]

    name_list = [ u'{0}'.format(n) for n in name.split(".")]
    ts        = expand_graphite_wildcard_metric_name.cache_timestamp
    cache_tree = expand_graphite_wildcard_metric_name.cache_tree
    if ts == 0 or (time.time() - ts > cache_ttl):
        all_metric_name_list = metadata.get_all_metric_names(conn)
        cache_tree           = tree()
        _make_graphite_name_cache(cache_tree, all_metric_name_list)
        expand_graphite_wildcard_metric_name.cache_tree      = cache_tree
        expand_graphite_wildcard_metric_name.cache_timestamp = time.time()
    expanded_name_list = metric_name_wildcard_expansion(cache_tree, name_list)
    return [ u".".join(en) for en in expanded_name_list]

expand_graphite_wildcard_metric_name.cache_tree = tree()
expand_graphite_wildcard_metric_name.cache_timestamp = 0
