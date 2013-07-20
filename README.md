pyKairosDB
==========

Python client library for KairosDB


KairosDB
========

https://github.com/proofpoint/kairosdb

References
==========

The documentation can be found at:

http://code.google.com/p/kairosdb/wiki/GettingStarted?tm=6

Features
========

* This module will provide the REST protocol client to KairosDB for
  inserting data and for querying data.  The telnet protocol is
  eschewed since it only provides a write API.

* KairosDB stores its timestamps as ms since the epoch.  Pythons time
  module deals with times as seconds since the epoch, with higher
  resolution time as a floating point value.  This module expects a
  python timestamp, and will translate the python timestamp to a
  kairosdb timestamp on reads and writes


TODO
====
* Since json can be slow, and since a large amount of data may be
  gathered, yajl should be used here since the documents being
  sent/received should be fairly
  simple. https://github.com/pykler/yajl-py looks good.

* Kairos reads and returns data as JSON via the rest API.  Support the
  gzip feature for bulk uploading: http://code.google.com/p/kairosdb/wiki/Features

* Add convenience functions that'll make it easier to create metrics
  in the correct format.

Working with Graphite
=====================

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

The grahite webui will be able to read the "storage-schema-retentions"
list of tags that comes back from the server, and it'll use the lowest
resolution data for the period being queried to generate the resulting
graph data.
