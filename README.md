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