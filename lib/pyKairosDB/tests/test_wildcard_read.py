#!/usr/bin/env python

import pyKairosDB
from pyKairosDB import util as util

# use this after the following read test has settled:

# ipython pyKairosDB/tests/test_graphite_write.py test.bar.baz
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.bar
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.foo
# ipython pyKairosDB/tests/test_graphite_write.py test.bar.bat
#

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl

first_metrics_list = pyKairosDB.util.expand_graphite_wildcard_metric_name(c, "test.*.*")
second_metrics_list = pyKairosDB.util.expand_graphite_wildcard_metric_name(c, "test.bar.*")
assert(first_metrics_list == second_metrics_list)

# print second_metrics_list

content = c.read_relative(first_metrics_list, (1, 'days'))
print content
