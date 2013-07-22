pyKairosDB
==========

Python client library for KairosDB


KairosDB
========

https://github.com/proofpoint/kairosdb

Features
========

* This module provides a basicREST protocol client to KairosDB for
  inserting data and for querying data.  The telnet protocol is
  eschewed since it only provides a write API.

* KairosDB stores its timestamps as ms since the epoch.  Pythons time
  module deals with times as seconds since the epoch, with higher
  resolution time as a floating point value.  This module expects a
  python timestamp, and will translate the python timestamp to a
  kairosdb timestamp on reads and writes


TODO
====

* setup.py

* Support mixtures of absolute and relative times in queries.
  Currently if a query is relative, both its start and end times must
  be expressed as relative times.  Same for absolutes.

* Since json can be slow, and since a large amount of data may be
  gathered, evaluate whether yajl should be used here since the
  documents being sent/received should be fairly
  simple. https://github.com/pykler/yajl-py perhaps?

* Kairos reads and returns data as JSON via the rest API.  Look into
  support the gzip feature for bulk uploading/downloading of a ton of
  metrics.  http://code.google.com/p/kairosdb/wiki/Features

Examples
========

To connect to a KairosDB instance running on localhost 8080, you can
connect using a zero-argument constructor:

```
connection = pyKairosDB.connect()
```
"connection" can now be used for basic read/write operations.

```
content = connection.read_relative(['test'], (1, 'days'))
```

This will get you 1 day's worth of data for a metric called "test".

Getting metadata:
```
print pyKairosDB.metadata.get_all_metric_names(connection)
```
This will print all of the metric names stored in your kairosdb backend


Working with Graphite
=====================

API for writing graphite metrics:

```
import pyKairosDB
import time
import sys
c = pyKairosDB.connect() # use localhost:8080, the default, no ssl
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
for m in range(5):
    graphite_metric_list.append((metric_name, time.time(), time.time(),))
    time.sleep(0.1)

r = c.write_metrics(pyKairosDB.graphite.graphite_metric_list_to_kairosdb_list(graphite_metric_list, graphite_tags_60s_1d))
```

API for reading graphite-style metrics:

```
import pyKairosDB
from pyKairosDB import graphite as pyk_graphite
(time_info, values) = pyk_graphite.read_absolute(self.conn, self.metric_path, startTime, endTime)
```

Graphite has two requirements for a data store.  The first is that the
data it receives be in uniform steps.  It defines the retentions in
its configuration file "storage-schemas.conf", where you say that some
metric should be saved via a set of rules that define its retention
policy.

For the purpose of using KairosDB with Graphite, it seems like the
best way to store this is in the tags associated with the metric being
sent.

So every metric that graphite will query MUST have the following tags,
all lower-case:

```
"graphite" : "true",
"storage-schema-name" : "default_1min_for_1day"
"storage-schema-retentions" : "60s:1d"
```

The grahite-web ui reads the "storage-schema-retentions"
list of tags that comes back from the server, and it'll use the lowest
resolution data for the period being queried to generate the resulting
graph data.

Since KairosDB stores data potentially forever, it doesn't focus on
summarization.  Because of that, when writing metrics to kairosdb, the
sender should set the storage-schema-retentions to the highest
resolution available in storage-schemas.conf since that will actually
be what's retained in kairosdb.  A separate process should be created
to retire un-needed metrics.  This means that if a storage-schema.conf
contains "60s:1d,5m:4w,1h:1y" only the 60s:1d should be sent to kairosdb.

References
==========

The documentation can be found at:

http://code.google.com/p/kairosdb/wiki/GettingStarted?tm=6
