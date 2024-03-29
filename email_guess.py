import requests
from parse import Parse
from parse import Prospecter
from google import Google
import random
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import arrow
import json
from press_sources import PRNewsWire
from press_sources import BusinessWire
from sources import Sources
from queue import RQueue
from toofr import Toofr
from webhook import Webhook
#from score import Score
import time
#from email_guess_helper import EmailGuessHelper

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class EmailGuess:
    def streaming_search(self, domain):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))

        for link in pw.link: 
            pn_emails = PRNewsWire()._find_emails(domain, link, False)
        for link in bw.link: 
            bw_emails = BusinessWire()._find_emails(domain, link, False)
        ''' enqueue and return values ''' 
        return pd.concat([pn_emails, bw_emails]).drop_duplicates('pattern')

    def start_search(self, domain):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))
        job_queue_lol = domain+str(arrow.now().timestamp)
        if not pw.empty:
            for link in pw.link: 
                print "PW STARTED", pw.shape, link
                job = q.enqueue(PRNewsWire()._find_emails, domain, link,
                                job_queue_lol, timeout=3600)
                job.meta['profile_id1'] = job_queue_lol
                job.save()
        print len(q.jobs)
        if not bw.empty:
            for link in bw.link: 
                print "BW STARTED", bw.shape, link
                job = q.enqueue(BusinessWire()._find_emails, domain, link,
                                job_queue_lol, timeout=3600)
                job.meta['profile_id1'] = job_queue_lol
                job.save()
        print len(q.jobs)
              
    def search_webhook(self, domain, objectId):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))
        job_queue_lol = objectId+str(arrow.now().timestamp)

        if not pw.empty:
            for link in pw.link: 
                print "PW STARTED", pw.shape, link
                job = q.enqueue(PRNewsWire()._email_webhook, domain, link,
                                job_queue_lol, objectId, timeout=3600)
                job.meta['profile_id1'] = job_queue_lol
                job.save()
        print len(q.jobs)

        if not bw.empty:
            for link in bw.link: 
                print "BW STARTED", bw.shape, link
                job = q.enqueue(BusinessWire()._email_webhook, domain, link,
                                job_queue_lol, objectId, timeout=3600)
                job.meta['profile_id1'] = job_queue_lol
                job.save()
        print len(q.jobs)

    def new_sources(self, domain, api_key, name=""):
        # TODO - First search jigsaw
        # If that fails search everything else
        email_pattern = Sources()._jigsaw_search(domain)
        if email_pattern != {}:
            ''' Webhook() '''
        else:
            q.enqueue(EmailGuess.search_sources, domain, api_key, name)
            
    def _find_if_object_exists(self, class_name, column, value, data):
        qry = json.dumps({column: value})
        obj = Parse().get(class_name, {'where': qry}).json()['results']
        print obj
        if obj: 
            print "NEW UPDATE OLD", class_name+'/'+obj[0]['objectId']
            print Parse().update(class_name+'/'+obj[0]['objectId'], data).json()
        else: 
            print "NEW CREATE NEW"
            print Parse().create(class_name, data).json()

    def search_sources(self, domain, api_key, name=""):
        pattern = Toofr().get(domain)
        if pattern: 
            ptn = {"domain":domain, "company_email_pattern":[{"source":"toofr", "pattern":pattern}] }
            self._find_if_object_exists('EmailPattern','domain', domain, ptn)
            Webhook()._update_company_email_pattern(ptn)
            return pattern

        # syncronous jigsaw search
        # job_5 = q.enqueue(Sources()._jigsaw_search, domain)
        job_1 = q.enqueue(Sources()._whois_search, domain)
        job_2 = q.enqueue(Sources()._google_span_search, domain)
        job_3 = q.enqueue(Sources()._press_search, domain, api_key)
        job_4 = q.enqueue(Sources()._zoominfo_search, domain)
        jobs = [job_1, job_2, job_3, job_4]
        if name != "":
            job_5 = q.enqueue(Sources()._mx_server_check, name, domain)
            job_6 = q.enqueue(Sources()._linkedin_login_search, name, domain)
            jobs = jobs + [job_5, job_6]

        for job in jobs:
            RQueue()._meta(job, "{0}_{1}".format(domain, api_key))


    def _random(self):
      qry = {'order':'-createdAt'}
      patterns= Prospecter().get('EmailPattern', qry).json()['results']
      email_guesses = []
      for count, pattern in enumerate(patterns):
          data = {'pattern': pattern['pattern'],
                 'tried': False,
                 'source':'random_guess'}
          email_guesses.append(data)
      random.shuffle(email_guesses)
      return email_guesses
