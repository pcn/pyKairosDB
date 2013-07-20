# -*- python -*-

"""
Per https://code.google.com/p/kairosdb/wiki/QueryingData

format for an absolute start time:
{
  "start_absolute":1,
  "metrics": [
    {
      "name": "archive.file.tracked",
    }
  ]
}
format for a relative start time:
{
  "start_relative":{"value":20,"unit":"weeks"},
  "metrics": [
    {
      "name": "archive.file.tracked",
    }
  ]
}

And per the docs, end_absolute and end_relative can be specified "in the same way".
"""

import requests
import json

# If you evoke an error:
# Out[7]: '{"errors":["\\"day\\" is not a valid time unit, must be one of MILLISECONDS,SECONDS,MINUTES,HOURS,DAYS,WEEKS,MONTHS,YEARS"]}'
VALID_UNITS = ("milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", "years")

def default_group_by():
    """Returns a dict that will have an group_by that can be modified"""
    return {
              "name": "time",
              "group_count": "1",
              "range_size": {
                "value": "1",
                "unit": "minutes"
              }
            }

def default_aggregator():
    """Returns a dict that will have an aggregator that can be modified"""
    return {
              "name": "avg",
              "sampling": {
                "value": "1",
                "unit": "minutes"
               }
           }


def group_by(group_by_list, metric_name, query_dict):
    """
    :type group_by_list: list
    :param group_by_list: a list of group-by clauses, e.g: the list within this:

         "group_by": [ {
              "name": "time",
              "group_count": "1",
              "range_size": {
                "value": "1",
                "unit": "minutes"
              }
            } ],

    should result in grouping/bucketing of values that would be
    friendly for a graphite data series that used a reporting period
    of a minute (along with aggreagtion on the minute).
    """
    print query_dict
    query_dict["metrics"][0]["group_by"] = group_by_list

def aggregation(aggregation_list, metric_name, query_dict):
    """
    :type aggregation_list: list
    :param aggregation_list: A list of aggregation clauses.  E.g. the list within this:
          "aggregators": [ {
              "name": "avg",
              "sampling": {
                "value": "1",
                "unit": "minutes"
               }
           } ]

    Combining this with a minutes group-by gives us datasets that graphite will like.
    """
    query_dict["metrics"][0]["aggregators"] = aggregation_list

def cache_time(cache_time, query_dict):
    """
    :type cache_time: int
    :param cache_time: If desired, the number of seconds for kairos to save the query for re-use

    Combining this with a minutes group-by gives us datasets that graphite will like.
    """
    query_dict["cache_time"] = cache_time


def read(conn, metric_names, start_absolute=None, start_relative=None,
         end_absolute=None, end_relative=None, query_modifying_function=None):
    """:type conn: pyKairosDB.connect object
    :param conn: the interface to the requests library

    :type metric_names: list
    :param metric_names: A list of names that will be retrieved.

    :type start_absolute: float
    :param start_absolute: This is the absolute start time (unix time since the epoch) for the batch of metrics being retrieved.

    :type start_relative: tuple
    :param start_relative: a tuple of (int or float) and a string.
                           The (int or float) contains the quantity of the time units that will be retrieved.
                           The string is the unit, containing "seconds", "minutes",
                          "hours", "days", "weeks", "months", or "years".
    :type end_absolute: float
    :param end_absolute: This is the absolute start time (unix time since the epoch) for the batch of metrics being retrieved.

    :type end_relative: tuple
    :param end_relative: a tuple of (int or float) and a string.
                           The (int or float) contains the quantity of the time units that will be retrieved.
                           The string is the unit, containing "seconds", "minutes",
                          "hours", "days", "weeks", "months", or "years".

    :type query_modifying_function: callable
    :param query_modifying_function: A function that will be given the query, and will modify it as needed.

    :rtype: dict
    :return: a dictionary that reflects the json returned from the kairosdb.

    """
    if start_relative is not None:
        query = _query_relative(start_relative, end_relative)
    elif start_absolute is not None:
        query = _query_absolute(start_absolute, end_absolute)
    query["metrics"] = [ {"name" : m } for m in metric_names ]
    if query_modifying_function is not None:
        query_modifying_function(query)
    r = requests.post(conn.read_url, json.dumps(query))
    return _change_timestamps_to_python(r.content)

def _query_relative(start, end=None):
    """
    :type start_relative: tuple
    :param start_relative: a tuple of (int or float) and a string.
                           The (int or float) contains the quantity of the time units that will be retrieved.
                           The string is the unit, containing "seconds", "minutes",
                          "hours", "days", "weeks", "months", or "years".

    :type end_relative: tuple
    :param end_relative: a tuple of (int or float) and a string.
                           The (int or float) contains the quantity of the time units that will be retrieved.
                           The string is the unit, containing "seconds", "minutes",
                          "hours", "days", "weeks", "months", or "years".
    """
    start_time = start[0] # This is here to confirm that the metric can be interpreted as a numeric
    start_unit = start[1]
    if start_unit not in VALID_UNITS:
        raise TypeError, "The time unit provided for the start time is not a valid unit: {0}".format(start)

    query = {
        "start_relative" : {"value": start_time, "unit": start_unit }
    }

    if end is not None:
        end_time = end[0]
        end_unit = end[1]
        if end_unit not in VALID_UNITS:
            raise TypeError, "The time unit provided for the end time is not a valid unit: {0}".format(end)
        query["end_relative"] = {"value" : end_time, "unit" : end_unit}

    return query

def _query_absolute(start, end):
    """
    :type start_absolute: float
    :param start_absolute: This is the absolute start time (unix time since the epoch) for the batch of metrics being retrieved.

    :type end_absolute: float
    :param end_absolute: This is the absolute start time (unix time since the epoch) for the batch of metrics being retrieved.
    """
    start_time = float(start) # This is here to confirm that the metric can be interpreted as a numeric

    query = {
        "start_absolute" : int(start * 1000)
    }

    if end is not None:
        end_time = float(end)
        query["end_absolute"] = int(end * 1000)

    return query


def read_relative(conn, metric_names, start, end=None, tags=None, query_modifying_function=None):
    """If end_relative is empty, "now" is implied"""
    return read(conn, metric_names, start_relative=start, end_relative=end,
        query_modifying_function=query_modifying_function)

def read_absolute(conn, metric_names, start, end=None, tags=None, query_modifying_function=None):
    """If end_absolute is empty, time.time() is implied"""
    return read(conn, metric_names, start_absolute=start, end_absolute=end,
        query_modifying_function=query_modifying_function)


def _change_timestamps_to_python(content):
    """Change timestamps from millis since the epoch to seconds since
    the epoch, with millisecond resolution

    An example of what will be in content is:
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
    """
    c_dict = json.loads(content)
    for q in c_dict["queries"]:
        for r in q["results"]:
            for v in r["values"]:
                v[0] = float(v[0]) / 1000.0
    return c_dict
