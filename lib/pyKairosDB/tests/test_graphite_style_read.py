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
(timeinfo, datapoints) = graphite.read_absolute(c, metrics_list[0], start_time, end_time)
print "Datapoints are:"
print datapoints
print "Timeinfo is:"
print timeinfo