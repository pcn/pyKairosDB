# -*- python -*-

import requests
from . import writer
from . import reader
from . import metadata
from . import graphite

class KairosDBConnection(object):

    def __init__(self, server='localhost', port='8080', ssl=False):
        self.ssl  = ssl
        self.server = server
        self.port = port

        # http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
        metadata.get_server_version(self) # XXX check for failure to connect
        self.generate_urls()


    def generate_urls(self):
        """This will allow the schema (http/https) to be changed during testing."""
        if self.ssl is True:
            self.schema = "https"
        else:
            self.schema = "http"
        self.read_url = "{0}://{1}:{2}/api/v1/datapoints/query".format(self.schema, self.server, self.port)
        self.write_url = "{0}://{1}:{2}/api/v1/datapoints".format(self.schema, self.server, self.port)

    def write_one_metric(self, name, timestamp, value, tags=None):
        return writer.write_one_metric(self, name, timestamp, value, tags)

    def write_metrics(self, metric_list):
        return writer.write_metrics_list(self, metric_list)

    def read_relative(self, metric_names_list, start, end=None, query_modifying_function=None):
        return reader.read_relative(self, metric_names_list, start, end,
            query_modifying_function=query_modifying_function)

    def read_absolute(self, metric_names_list, start, end=None, query_modifying_function=None):
        return reader.read_absolute(self, metric_names_list, start, end,
            query_modifying_function=query_modifying_function)
