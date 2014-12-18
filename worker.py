import os
import redis
from rq import Worker, Queue, Connection
#heroku worker

listen = ['high', 'default', 'low']
'''
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
#redis_url = os.getenv('REDISCLOUD_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)
'''

url = urlparse.urlparse(os.environ.get('REDISCLOUD_URL'))
conn = redis.Redis(host=url.hostname, port=url.port, password=url.password)

if __name__ == '__main__':
  with Connection(conn):
    worker = Worker(map(Queue, listen))
    worker.work()
