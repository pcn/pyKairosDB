#!/usr/bin/env python

import pyKairosDB
import time
import sys

# use this after a read test

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl
# create 5 metrics
if sys.argv > 1:
  metric_name = sys.argv[1]
else:
  metric_name = "name"
graphite_metric_list = list()
for m in range(5):
    graphite_metric_list.append((metric_name, time.time(), time.time(),))
    time.sleep(0.1)

r = c.write_metrics(pyKairosDB.util.graphite_metric_list_to_kairosdb_list(graphite_metric_list))
