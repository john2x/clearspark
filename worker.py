import os
import redis
from rq import Worker, Queue, Connection
import urlparse
import getopt
import sys
from multiprocessing import Process

# Preload heavy libraries
from bs4 import BeautifulSoup
import pandas as pd
import requests

concurrency = 1

try:
  opts, args = getopt.getopt(sys.argv[1:],"c:",[])
except getopt.GetoptError:
  print 'worker.py -c <concurrency>'
  sys.exit(2)
for opt, arg in opts:
  if opt == '-c':
    concurrency = int(arg)

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISCLOUD_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

def work():
  with Connection(conn):
    Worker(map(Queue, listen)).work()

if __name__ == '__main__':
  processes = [Process(target=work) for x in range(concurrency)]
  for process in processes:
    process.start()
  for process in processes:
    process.join()
