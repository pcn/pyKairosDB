#!/usr/bin/env python

"""Functions for getting metadata from the server"""

import requests
import json

def get_server_version(conn):
    """
    :type conn: KairosDBConnection
    :param conn: a connection object

    :rtype: string
    :return: String containing the version of the KairosDB server.
    """
    version_path = "api/v1/version"
    return json.loads(requests.get("{0.schema}://{0.server}:{0.port}/{1}".format(conn, version_path)).content)['version']

def get_all_metric_names(conn):
    """
    :type conn: KairosDBConnection
    :param conn: a connection object

    :rtype: list
    :return: list containing the strings of all of the metric names that the server has recorded.
    """
    all_names_path = "api/v1/metricnames"
    return json.loads(requests.get("{0.schema}://{0.server}:{0.port}/{1}".format(conn, all_names_path)).content)['results']