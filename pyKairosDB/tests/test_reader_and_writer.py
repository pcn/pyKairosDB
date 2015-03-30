# -*- python -*-

import time
import unittest

import pyKairosDB
from pyKairosDB import util as util

# use this after a read test

#c = pyKairosDB.connect() # use localhost:8080, the default, no ssl

class TestReadAndDelete(unittest.TestCase):
    def setUp(self):
        print "setUp"
        self.conn = pyKairosDB.connect('192.168.0.116', '8080')
        self.write_delay = 0.5
        self.metric_name = 'Metric11'
        self.metrics = [{'name': self.metric_name,
                         'timestamp': time.time(),
                         'value': 1,
                         'tags': {'resource_id': 123}},

                        {'name': self.metric_name,
                         'timestamp': time.time() + 1,
                         'value': 2,
                         'tags': {'resource_id': 123}},

                        {'name': self.metric_name,
                         'timestamp': time.time() + 2,
                         'value': 3,
                         'tags': {'resource_id': 123}},

                        {'name': self.metric_name,
                         'timestamp': time.time() + 3,
                         'value': 4,
                         'tags': {'resource_id': 1}},

                        {'name': 123,
                         'timestamp': time.time() + 3,
                         'value': 5,
                         'tags': {'resource_id': 1}}]

        self.conn.write_metrics(self.metrics)
        time.sleep(self.write_delay)

    def _get_modifying_function(self, aggregation='dev', granularity=10):

        def func(query):
            if query is not None:
                query['metrics'][0]['aggregators'] = [{'name': aggregation}]
                query['metrics'][0]['aggregators'][0]['sampling'] = {
                    'value': granularity, 'unit': 'seconds'}
        return func

    def tearDown(self):
        print "tearDown"
        self.conn.delete_metrics([self.metric_name])
        time.sleep(self.write_delay)
        self.conn = None

    def test_read_relative_without_tags(self):
        r = self.conn.read_relative([self.metric_name], start_time=(1, 'days'))
        print "test_read_relative_without_tags: ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_relative_with_tags(self):
        r = self.conn.read_relative([self.metric_name], start_time=(1, 'days'),
                                    tags={'resource_id': 123})
        print "test_read_relative_with_tags: ", r
        self.assertEqual(r['queries'][0]['sample_size'], 3)

    def test_read_relative_with_empty_tags(self):
        time.sleep(self.write_delay)
        r = self.conn.read_relative([self.metric_name], start_time=(1, 'days'),
                                    tags={})
        print "test_read_relative_with_empty_tags ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_relative_only_read_tags(self):
        time.sleep(self.write_delay)
        r = self.conn.read_relative([self.metric_name], start_time=(1, 'days'),
                                    only_read_tags=True, tags={'resource_id': '123'})

        print "test_read_relative_only_read_tags ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_relative_with_aggregation(self):
        func = self._get_modifying_function()
        r = self.conn.read_relative([self.metric_name], start_time=(1, 'days'),
                                    query_modifying_function=func)
        print "test_read_relative_with_aggregation: ", r
        self.assertEqual(len(r['queries'][0]['results'][0]['values']), 2)

    def test_read_absolute_with_tags(self):
        r = self.conn.read_absolute([self.metric_name], start_time=0,
                                    tags={'resource_id': 123})
        print "test_read_absolute_with_tags: ", r
        self.assertEqual(r['queries'][0]['sample_size'], 3)

    def test_read_absolute_without_tags(self):
        r = self.conn.read_absolute([self.metric_name], start_time=0)
        print "test_read_absolute_without_tags: ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_absolute_with_empty_tags(self):
        time.sleep(self.write_delay)
        r = self.conn.read_absolute([self.metric_name], start_time=0, tags={})
        print "test_read_absolute_with_empty_tags ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_absolute_only_read_tags(self):
        time.sleep(self.write_delay)
        r = self.conn.read_absolute([self.metric_name], start_time=0,
                                    only_read_tags=True, tags={'resource_id': '123'})

        print "test_read_absolute_only_read_tags ", r
        self.assertEqual(r['queries'][0]['sample_size'], 4)

    def test_read_absolute_with_aggregation(self):
        func = self._get_modifying_function()
        r = self.conn.read_absolute([self.metric_name], start_time=0,
                                    query_modifying_function=func)
        print "test_read_absolute_with_aggregation: ", r
        self.assertEqual(len(r['queries'][0]['results'][0]['values']), 1)

    def test_delete_datapoints(self):
        time.sleep(self.write_delay)
        self.conn.delete_datapoints([self.metric_name], start_time=0,
                                    tags={'resource_id': 1})
        time.sleep(self.write_delay)
        r = self.conn.read_absolute([self.metric_name], start_time=0)
        print "test_delete_datapoints: ", r
        self.assertEqual(r['queries'][0]['sample_size'], 3)

if __name__ == '__main__':
    unittest.main()

