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

graphite_tags_60s_1d = {
  "graphite" : "true",
  "storage-schema-name" : "default_1min_for_1day",
  "storage-schema-retentions" : "60s:1d"
}

graphite_tags_10s_1w = {
  "graphite" : "true",
  "storage-schema-name" : "default_1min_for_1day",
  "storage-schema-retentions" : "10s:1w"
}

graphite_tags_30s_1y = {
  "graphite" : "true",
  "storage-schema-name" : "default_1min_for_1day",
  "storage-schema-retentions" : "30s:1y"
}

for m in range(5):
    graphite_metric_list.append((metric_name, time.time(), time.time(),))
    time.sleep(0.1)

r = c.write_metrics(pyKairosDB.graphite.graphite_metric_list_to_kairosdb_list(graphite_metric_list, graphite_tags_60s_1d))
