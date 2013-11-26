# -*- python -*-

"""This module contains ease-of-use functions"""
import sys
import time
import itertools
import fnmatch

from collections import defaultdict
from . import metadata as metadata

# import macropy

# Nice hack: https://gist.github.com/hrldcpr/2012250
def tree():
  """
  This will create a tree of defaultdicts... of trees.  This is a
  nice hack, and auto-vivifies the necessary dicts.
  """
  return defaultdict(tree)

def get_content_values_by_name(content, name):
    """
    :type content: dict
    :param content: A dictionary as returned by KairosDB with timestamps converted to seconds since the epoch

    :type name: str
    :param name: The name of the entry in the results whose values will be returned

    :rtype: list
    :return: a list of resulting [timestamp, value] pairs

    When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    return the dict(s) whose name matches the argument "name"

    """
    r_list = list()
    for q in content["queries"]:
        for r in q["results"]:
            if r["name"] == name:
                r_list.append(r)
    return r_list

def content_by_name_substring(content, name):
    """
    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type name: string
    :param name: This is the string that will be used to match, as a lowercase substring, the things that we want to return.

    :rtype: list
    :returns: The list of pairs of [timestamp, value] that matched

    When you've got content, but only want to look at a piece of it,
    specifically the values that are provided for a particular name, then use
    this function.

    and this will be turned into a python struct.  This function will
    return a list of dicts that matched the provided name.

    """
    r_list = list()
    for q in content["queries"]:
        for r in q["results"]:
            if name.lower() in r["name"].lower():
                r_list.append(r)
    return r_list


def get_matching_tag_values(content, tag_key, tag_value_list):
    """
    :type content: dict
    :param content: The python dict form of the content that has been returned from a query to KairosDB

    :type tag_key: list
    :param tag_key: This will index the tag that we're interested in

    :type tag_value_list: list
    :param tag_value_list: list of strings, each of which is a possible tag

    :rtype: list
    :return: a list of result dicts that matched

    When you've got content, but only want to look at a piece of
    it, specifically the values that are provided which match one more
    tag key and some number of tag values, then use this function.

    In short, this tells you whether a tags key and value are in a
    response.
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

def get_matching_tags_from_result(result, tag_key):
    """
    :type result: dict
    :param result: The python dict form of one value as returned in the result content

    :type tag_key_list: list
    :param tag_key_ist: This is a list of indexes of the tags that we're interested in

    :type tag_value_list: list
    :param tag_value_list: list of strings, each of which is a possible tag

    :rtype: list
    :return: a list of tag values (strings) that matched

    When you've got content, but only want to look at a piece of
    it, specifically the values that are provided which match one more
    tag key and some number of tag values, then use this function.

    This gets the tag and returns its tag values.
    """
    r_set = set()
    # print result
    # print
    # print
    if tag_key.lower() in result["tags"].keys():
        return result["tags"][tag_key]


def _add_to_cache(cache_tree, list_of_names):
    """
    :type cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function.  Contains names
                             of entries in the kairosdb, separated by "." per the graphite convention.
                             This is modified in-place

    :type list_of_names: list
    :description list_of_names: list of strings, in order, that will be sought after in the cache tree.

    This should probably be done via macropy, I need to understand
    more about how that'd work first.  I'd like to have macro expansion
    of "list_of_names", e.g. ['a', 'b', 'c'] to turn into
    cache_tree['a']['b']['c']

    Cache_tree is a tree of dictionaries, so it will be mutated in
    place.

    """
    head_item = list_of_names[0]
    cache_tree[head_item]
    if len(list_of_names) == 1:
        return
    else:
        tail_list = list_of_names[1:]
        _add_to_cache(cache_tree[head_item], tail_list)


def _match_in_cache(cache_tree, list_of_names):
    """
    :type cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function.  Contains names
                             of entries in the kairosdb, separated by "." per the graphite convention.

    :type list_of_names: list
    :description list_of_names: list of strings, in order, that will be sought after in the cache tree.

    :rtype: list
    :return: A list of matches, possibly empty.

    Given a cache_tree, and a prefix, returns all of the values associated with that prefix,
    that is, the keys that reside under the prefix.
    """
    head_item = list_of_names[0]
    # print "head_item is {0}".format(head_item)
    head_item_matches = [ m for m in cache_tree.keys() if fnmatch.fnmatch(m, head_item) ]
    if head_item not in cache_tree.keys():
        # print "A"
        return [] # Empty List to signify we're done here
    elif len(list_of_names) == 1:
        # print "B"
        return cache_tree[head_item].keys()
    else:
        # print "C"
        tail_list = list_of_names[1:]
        return _match_in_cache(cache_tree[head_item], tail_list)

def _not_metric_name_wildcard_expansion(cache_tree, name_list):
    """
    :param cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function

    :param name_list: list
    :description name_list: a list of strings created by splitting a name around e.g. "." boundaries (per graphite)

    :rtype: list
    :return: a list of lists.  Each sub-list is the split-out metric names.

    Given some metrics, break up the list around wildcard entries and
    return all those that match.

    This only supports wildcards that are standalone - e.g. I don't
    know whether or not the web ui expects "foo.* and foo.a* to both
    work (the former I know works, the latter, well...?) but this will
    only deal with an * surrounded by a wildcard.

    This only returns the prefixes implied by the *.   E.g. for the list::

        ['a', 'b', '*']

    and the tree::

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

    applying the name_list "a.b.d.*" to the cache_tree will return::

        [['a', 'b', 'd', 'a']], [['a', 'b', 'd', 'b']], [['a', 'b', 'd', 'c']]

    """
    wildcard_char="*"
    count = 0
    complete_list = list()
    # print "name_list is {0}".format(name_list)
    for n in range(len(name_list)):
        if name_list[n] == wildcard_char:
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

def _metric_name_wildcard_expansion(cache_tree, name_list):
    """
    :param cache_tree: defaultdict
    :description cache_tree: a defaultdict initialized with the tree() function

    :param name_list: list
    :description name_list: a list of strings created by splitting a name around e.g. "." boundaries (per graphite)

    :rtype: list
    :return: a list of lists.  Each sub-list is the split-out metric names.

    Given some metrics, break up the list around wildcard entries and
    return all those that match.

    This supports wildcards that match fnmatch rules.

    This only returns the prefixes implied by the *.   E.g. for the list::

        ['a', 'b', '*']

    and the tree::

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

    applying the name_list "a.b.d.*" to the cache_tree will return::

        [['a', 'b', 'd', 'a']], [['a', 'b', 'd', 'b']], [['a', 'b', 'd', 'c']]

    """
    if len(name_list) == 0: # Catch an empty list being passed in.
        return []
    complete_list = list()
    names_head = name_list[0]
    names_rest = name_list[1:]
    matches = [ n for n in cache_tree.keys() if fnmatch.fnmatch(n, names_head) ]
    # print "matches are {0}".format(matches)
    for m in matches:
        # print "m is {0}".format(m)
        this_list = [m]
        expansion = _metric_name_wildcard_expansion(cache_tree[m], names_rest)
        # print "Expansion is {0}".format(expansion)
        for e in expansion:
            this_list.extend(expansion)
            # print "This_list is now {0}".format(this_list)
        complete_list.extend([this_list])
    # print "Returning complete_list of {0}".format(complete_list)
    return complete_list

def _almost_flatten(metrics):
    """Turn a nested list (e.g. ['foo', ['bar', 'baz', ['tor, 'tar']]] into
    a flattened list of names anchoered at the first element:
    [["foo", "bar", "baz", "tor"],
     ["foo", "bar", "baz", "tar"]]
    """
    metric_part_list = list()
    metric_head = metrics[0]
    metric_tail = metrics[1:]
    # print "metric_head, metric_tail: {0}, {1}".format(metric_head, metric_tail)
    if metric_tail == []:
        return [metric_head]
    for mt in metric_tail:
        result = _almost_flatten(mt)
        # print "result is {0}".format(result)
        for r in result:
            tail_list   = list()
            tail_list.append(metric_head)
            if type(r) in (type(list()), type(tuple())):
                tail_list.extend(r)
            else:
                tail_list.append(r)
            metric_part_list.append(tail_list)
    return metric_part_list



def metric_name_wildcard_expansion(cache_tree, name_list):
    metric_name_list = _metric_name_wildcard_expansion(cache_tree, name_list)
    return_list = list()
    for m in metric_name_list:
        # This is doing a lot of extra work, returning a lot of extra lists.
        # I may want a better way to do this at some point.
        # print "m is {0}".format(m)
        return_list.extend(_almost_flatten(m))
    return return_list
