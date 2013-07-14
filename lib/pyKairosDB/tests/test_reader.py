# -*- python -*-

import pyKairosDB

# use this after a read test

c = pyKairosDB.connect() # use localhost:8080, the default, no ssl

r = c.read_relative(['test'], (1, 'days'))

print r
