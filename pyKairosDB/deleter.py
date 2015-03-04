import json
import logging
import requests

from . import reader

LOG = logging.getLogger(__name__)

def delete_metric(conn, metric):
    delete_url = conn.delete_metric_url + str(metric)
    r = requests.delete(delete_url)
    if r.status_code != 204:
        LOG.exception('deletion of metric %s failed. Status code: %s') % (
            metric, r.status_code)
        raise

def delete_metrics(conn, metric_names_list):
    for metric in metric_names_list:
        delete_metric(conn, metric)

def delete_datapoints(conn, metric_names_list, start_time=0,
                      end_time=None,tags=None):
    query = reader._query_absolute(start=start_time, end=end_time)
    query["metrics"] = [{"name" : m } for m in metric_names_list]
    if tags:
        query = reader.add_tags_to_query(query, tags)
    delete_url = conn.delete_dps_url
    return requests.post(delete_url, json.dumps(query))
