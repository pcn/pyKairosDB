#!/usr/bin/env python

import pyKairosDB
import time

# use this after a read test

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl
r = c.write_one_metric("test", time.time(), time.time(), tags = {"graphite" : "yes"})
