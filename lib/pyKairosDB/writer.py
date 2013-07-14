# -*- python -*-

"""KairosDB expects a list of objects per this:
[{
    "name": "archive.file.tracked",
    "timestamp": 1349109376,
    "value": 123,
    "tags":{"host":"test"}
},
{
    "name": "archive.file.search",
    "timestamp": 999,
    "value": 321,
    "tags":{"host":"test"}
}]

The documentation for this is at https://code.google.com/p/kairosdb/wiki/PushingData

We send this via the reuests object that the KairosDBConnection presents.

"""

# import yajl_py as json
import json # change to using yajl when performance is needed.
import requests

def write_one_metric(conn, name, timestamp, value, tags):
    """
    construct a single metric and send it to kairosdb.  Does some duck-type checking on the tags.
    At least one tag must be present or kairosdb will not store the metric.

    :type conn: pyKairosDB.connect object
    :param conn: the interface to the requests library

    :type name: string
    :param name: The name of the metric to be sent

    :type timestamp: float
    :param timestamp: the unix-style timestamp (to a microsecond resolution) that will be sent for this metric

    :type value: float
    :param value: The value that will be sent along for this metric

    :rtype: request.response
    :return: a requests.response object with the results of the write

    """
    if 'keys' not in dir(tags):
        raise TypeError, "The tags provided doesn't look enough like a dict: {0} is type {1}".format(tags, type(tags))
    metric = {
        "name" : name,
        "timestamp" : timestamp,
        "value" : value,
        "tags" : tags
    }
    return write_metrics_list(conn, [metric])

def write_metrics_list(conn, metric_list):
    """Takes a list of formatted metrics hashes and writes them to the
    api.  This takes each timestamp, which should be a regular python
    time.time() in seconds since the epoch, and multiplies it by 1000,
    and posts the int version (no decimal)  to agree with what kairosdb
    expects.

    :type conn: pyKairosDB.connect object
    :param conn: The interface to the requests library

    :type metrics_list: list()
    :param metrics_list: list of dicts, each dict is a metric that will be sent to KairosDB

    :rtype: request.response
    :return: a requests.response object with the results of the write
    """
    for m in metric_list:
        m["timestamp"] = int(m["timestamp"] * 1000)

    metrics = json.dumps(metric_list)
    return requests.post(conn.write_url, metrics)
