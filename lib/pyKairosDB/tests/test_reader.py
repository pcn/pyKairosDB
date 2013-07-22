# -*- python -*-

import pyKairosDB
from pyKairosDB import util as util

# use this after a read test

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl

content = c.read_relative(['test'], (1, 'days'))
# print content
print util.get_content_values_by_name(content, "test")
