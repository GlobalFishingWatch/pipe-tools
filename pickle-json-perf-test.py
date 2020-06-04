import json
import ujson
import time
import pickle

# Some performance tests to compare difference approches to seriallizing
# dicts in apache beam

# wrapper class for a dict.  We need a unique class in dataflow to associate with
# a specific coder

class Message(dict):
  pass


d = dict(
  field_1= 'twas brillig and  the slithy toves',
  astro= 123456789012345,
  lat= 0.321,
  lon= 1.234,
  field_2= 'aaa bbb ccc ddd'
)

json_str = json.dumps(d)
pickle_str = pickle.dumps(d)

n=1000000

print("Runing perf tests with %s iterations..." % n)
start = time.time()
for i in range(n):
  d = pickle.loads(pickle_str)
print("%s pickle.loads" % (time.time() - start))

start = time.time()
for i in range(n):
  d = json.loads(json_str)
print("%s json.loads " % ( time.time() - start))

start = time.time()
for i in range(n):
  d = ujson.loads(json_str)
print("%s ujson.loads" % (time.time() - start))

start = time.time()
for i in range(n):
  d = ujson.loads(json_str)
  m = Message(d)
print("%s ujson.loads with assignment" % (time.time() - start))
