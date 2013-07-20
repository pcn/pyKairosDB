#!/usr/bin/env python

import time
import pyKairosDB
from pyKairosDB import util as util
from pyKairosDB import graphite

# use this after the following read test has settled:

# ipython pyKairosDB/tests/test_graphite_write.py test.bar.baz
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.bar
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.foo
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.bat
#

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl

start_time = time.time() - 3600
end_time   = time.time()

metrics_list = graphite.expand_graphite_wildcard_metric_name(c, "test.*.*.*")
# content = c.graphite.read_absolute([metrics_list[0]], start_time, end_time)
#
# # print "First, content is "
# # print content
#
# interval_seconds = graphite.get_lowest_resolution_graphite_retention_from_content(
#     content, metrics_list[0])
# group_by   = pyKairosDB.reader.default_group_by()
# group_by["range_size"] = { "value" : interval_seconds, "unit" : "seconds"}
#
# aggregator = pyKairosDB.reader.default_aggregator()
# aggregator["sampling"] = group_by["range_size"]
#
# content = c.read_absolute([metrics_list[0]], start_time, end_time,
#     group_by_list=[group_by], aggregation_list = [aggregator])
# # print "Second content is"
# # print content

content = graphite.read_absolute(c, metrics_list[0], start_time, end_time)
print content