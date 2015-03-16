# -*- python -*-

import requests
from . import writer
from . import reader
from . import metadata
from . import graphite
from . import deleter

class KairosDBConnection(object):
    """
    :type server: str
    :param server: the host to connect to that is running KairosDB
    :type port: str
    :param port: the port, as a string, that the KairosDB instance is running on
    :type ssl: bool
    :param ssl: Whether or not to use ssl for this connection.
    """

    def __init__(self, server='localhost', port='8080', ssl=False):
        """
        :type server: str
        :param server: the host to connect to that is running KairosDB
        :type port: str
        :param port: the port, as a string, that the KairosDB instance is running on
        :type ssl: bool
        :param ssl: Whether or not to use ssl for this connection.
        """
        self.ssl  = ssl
        self.server = server
        self.port = port

        # We shouldn't have to worry about efficient re-use of connections, pooling, etc. See:
        # http://docs.python-requests.org/en/latest/user/advanced/#keep-alive
        self._generate_urls()
        metadata.get_server_version(self) # XXX check for failure to connect



    def _generate_urls(self):
        """This method creates the URL each time a read or write operation is performed.

        By its nature, it allows the schema (http/https) to be changed e.g. if it's desired for testing.
        """
        if self.ssl is True:
            self.schema = "https"
        else:
            self.schema = "http"
        self.read_url     = "{0}://{1}:{2}/api/v1/datapoints/query".format(self.schema, self.server, self.port)
        self.read_tag_url = "{0}://{1}:{2}/api/v1/datapoints/query/tags".format(self.schema, self.server, self.port)
        self.write_url    = "{0}://{1}:{2}/api/v1/datapoints".format(self.schema, self.server, self.port)
        self.delete_dps_url = "{0}://{1}:{2}/api/v1/datapoints/delete".format(self.schema, self.server, self.port)
        self.delete_metric_url = "{0}://{1}:{2}/api/v1/metric/".format(self.schema, self.server, self.port)

    def write_one_metric(self, name, timestamp, value, tags):
        """
        :type name: str
        :param name: the name of the metric being written

        :type timestamp: float
        :param timestamp: the number of seconds since the epoch, as a float.  Per the return value of time.time()

        :type value: float
        :param value: The value of the metric to be recorded

        :type tags: dict
        :param tags: A dictionary of key : value strings that are the tags that will be recorded with this metric.

        :rtype: requests.response
        :return: a requests.response object with the results of the write

        This is the API for writing a single metric, making it
        easier in simple cases.  This method is inefficient for large
        batches, and write_metrics() should be used instead in these cases.

        """
        return writer.write_one_metric(self, name, timestamp, value, tags)

    def write_metrics(self, metric_list):
        """
        :type tags: list
        :param tags: list of dictionaries of metrics, including name, timestamp, value, and tags to be written

        :rtype: requests.response
        :return: a requests.response object with the results of the write
        """
        return writer.write_metrics_list(self, metric_list)

    def read_relative(self, metric_names_list, start_time, end_time=None,
                      query_modifying_function=None, only_read_tags=False, tags=None):
        """
        :type metric_names_list: list
        :param metric_names_list: list of metric names to be queried

        :type start_time: list
        :param start_time: The start time for this read request as a pair of values.  The first element
            is a number, the second element is a string specifying the unit of time, e.g. "days", "seconds", "hours", etc.

        :type end_time: list
        :param end_time: The end time for this read request as a pair of values.  The first element
            is a number, the second element is a string specifying the unit of time, e.g. "days", "seconds", "hours", etc.

        :type query_modifying_function: function
        :param query_modifying_function: A function that accepts one argument: the query being created.  It can be used
            to arbitrarily modify the contents of the request.  Intended for applying modifications to aggregators and
            grouping and caching when appropriate values for these are discovered.

        :type only_read_tags: bool
        :param only_read_tags: Whether the query will be for tags or for tags and data.  Default is both.

        :type tags: dict
        :param tags: Contains tags which will be added to the query. If only_read_tags=True, will filter the results to
            those that have specified tags.

        :rtype: requests.response
        :return: a requests.response object with the results of the write

        Read values for the requested metrics using a relative start time, and optionally a relative end time (or don't use
        an end time, which means "now")
        """
        return reader.read_relative(self, metric_names_list, start_time, end_time,
                                    query_modifying_function=query_modifying_function,
                                    only_read_tags=only_read_tags, tags=tags)

    def read_absolute(self, metric_names_list, start_time, end_time=None,
                      query_modifying_function=None, only_read_tags=False, tags=None):
        """
        :type metric_names_list: list
        :param metric_names_list: list of metric names to be queried

        :type start_time: float
        :param start_time: The start time for this read request as seconds since the epoch (per python's time.time())

        :type end_time: float
        :param end_time: The end time for this read request as seconds since the epoch (per python's time.time())

        :type query_modifying_function: function
        :param query_modifying_function: A function that accepts one argument: the query being created.  It can be used
            to arbitrarily modify the contents of the request.  Intended for applying modifications to aggregators and
            grouping and caching when appropriate values for these are discovered.

        :type only_read_tags: bool
        :param only_read_tags: Whether the query will be for tags or for tags and data.  Default is both.

        :type tags: dict
        :param tags: Contains tags which will be added to the query. If only_read_tags=True, will filter the results to
            those that have specified tags.

        :rtype: requests.response
        :return: a requests.response object with the results of the write

        Read values for the requested metrics using an absolute start time, and optionally an absolute end time (or don't use
        an end time, which means time.time())
        """
        return reader.read_absolute(self, metric_names_list, start_time, end_time,
                                    query_modifying_function=query_modifying_function,
                                    only_read_tags=only_read_tags, tags=tags)

    def delete_datapoints(self, metric_names_list, start_time, end_time=None, tags=None):
        """
        :type metric_names_list: list
        :param metric_names_list: list of metric names to be queried.

        :type start_time: float
        :param start_time: The start time for this read request as seconds since the epoch (per python's time.time())

        :type end_time: float
        :param end_time: The end time for this read request as seconds since the epoch (per python's time.time())

        :type only_read_tags: bool
        :param only_read_tags: Whether the query will be for tags or for tags and data.  Default is both.

        :type tags: dict
        :param tags: Tags to be searched in metrics. Allows to filter the results to only metric which contain specified
        tags in case only_read_tags=True.

        Performs the query made from specified parameters and deletes all data points returned by the query.
        Aggregators and groupers have no effect on which data points are deleted.
        Note: Works for the Cassandra and H2 data store only.
        """
        return deleter.delete_datapoints(self, metric_names_list, start_time, 
                                         end_time, tags=tags)

    def delete_metrics(self, metric_names_list):
        """
        :type metric_names_list: list
        :param metric_names_list: list of metric names to be deleted.
        """
        return deleter.delete_metrics(self, metric_names_list)
