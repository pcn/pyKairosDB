# -*- python -*-

import json
import logging
import requests

from . import reader

LOG = logging.getLogger(__name__)

def delete_metric(conn, metric):
    """Deletes metric and all it's datapoints from the database.

    :type conn: pyKairosDB.connect object
    :param conn: the interface to the requests library

    :type metric:
    :param metric: the metric name
    """
    delete_url = conn.delete_metric_url + str(metric)
    r = requests.delete(delete_url)
    if r.status_code != 204:
        LOG.exception('deletion of metric %s failed. Status code: %s') % (
            metric, r.status_code)
        raise

def delete_metrics(conn, metric_names_list):
    """
    :type conn: pyKairosDB.connect object
    :param conn: the interface to the requests library

    :type metric_names_list: list of str
    :param metric_names_list: A list of metric names to be deleted
    """
    for metric in metric_names_list:
        delete_metric(conn, metric)

def delete_datapoints(conn, metric_names_list, start_time,
                      end_time=None, tags=None):
    """Deletes data points.

    :type conn: pyKairosDB.connect object
    :param conn: the interface to the requests library

    :type metric_names_list: list of str
    :param metric_names_list: A list of metric names which datapoints will be deleted.

    :type start_time: float
    :param start_time: This is the absolute start time (unix time since the epoch) for the batch of metrics being retrieved
        and deleted afterwards.

    :type end_time: float
    :param end_time: This is the absolute end time (unix time since the epoch) for the batch of metrics being retrieved
        and deleted afterwards.

    :type tags: dict
    :param tags: Tags to be searched in metrics. Allows to filter the results to only metric which contain specified
        tags in case only_read_tags=True.

    First the query is created to retrieve the data points which will be deleted afterwards.
    """
    query = reader._query_absolute(start=start_time, end=end_time)
    query["metrics"] = [{"name" : m } for m in metric_names_list]
    if tags:
        query = reader.add_tags_to_query(query, tags)
    delete_url = conn.delete_dps_url
    return requests.post(delete_url, json.dumps(query))
