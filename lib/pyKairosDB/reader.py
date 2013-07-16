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


def read(conn, metric_names, start_absolute=None, start_relative=None, end_absolute=None, end_relative=None):
    """
    :type conn: pyKairosDB.connect object
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

    :rtype: dict
    :return: a dictionary that reflects the json returned from the kairosdb.
    """
    if start_relative is not None:
        query = _query_relative(start_relative, end_relative)
    elif start_absolute is not None:
        query = _query_absolute(start_absolute, end_absolute)
    query["metrics"] = [ {"name" : m } for m in metric_names ]
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




def read_relative(conn, metric_names, start, end=None):
    """If end_relative is empty, "now" is implied"""
    return read(conn, metric_names, start_relative=start, end_relative=end)


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
