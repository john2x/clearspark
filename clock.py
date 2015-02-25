from apscheduler.schedulers.blocking import BlockingScheduler
from social import *
from press import *

sched = BlockingScheduler()

# Social
@sched.scheduled_job('interval', minutes=5)
def facebook_posts():
    q.enqueue(Facebook()._recent)

@sched.scheduled_job('interval', minutes=5)
def twitter_posts():
    q.enqueue(Twitter()._recent)

@sched.scheduled_job('interval', minutes=5)
def linkedin_posts():
    q.enqueue(Linkedin()._recent)

@sched.scheduled_job('interval', minutes=5)
def glassdoor_reviews():
    q.enqueue(GlassDoor()._recent)

# Press Releases - Wait For Part 2
@sched.scheduled_job('interval', minutes=5)
def prnewswire():
  q.enqueue(Press()._recent)

# Jobs

# News

sched.start()

# Create News Feeds For Signals
'''
  Basically Going To Have A Giant Company Events DB
  Maybe Start storing events into SimpleDB as well
  Company Event

  ProspectList + CompanyProspectList News Feed
  - When Event is Added With Company
  - Search All Prospect | CompanyProspect with that company
  - Add All Lists To Event Instance
  - So event instance has lists attribute
  - 
'''
