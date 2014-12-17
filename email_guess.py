import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import arrow
import json
from press_sources import PRNewsWire
from press_sources import BusinessWire
from sources import Sources
from queue import RQueue
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

    def search_sources(self, domain, name=""):
        #job_1 = q.enqueue(Sources()._google_span_search, domain)
        job_2 = q.enqueue(Sources()._mx_server_check, name, domain)
        #job_3 = q.enqueue(Sources()._whois_search, domain)
        #job_4 = q.enqueue(Sources()._press_search, domain)
        #job_5 = q.enqueue(Sources()._zoominfo_harvest, domain)
        '''
        job_1.meta['domain'] = domain; job_1.save()
        job_2.meta['domain'] = domain; job_2.save()
        job_3.meta['domain'] = domain; job_3.save()
        job_4.meta['domain'] = domain; job_4.save()
        job_5.meta['domain'] = domain; job_5.save()
        '''

        print "Started"
        '''
        results = RQueue()._results(domain=domain)
        while None in results or results == []:
            print "whiling"
            time.sleep(1)
            results = RQueue()._results(domain=domain)
        '''

        print RQueue()._results(domain=domain)
        print "Listening"

        results = [result for result in RQueue()._results(domain=domain)
                  if type(result) is not str]
        results = pd.concat(results)
        # df should contain name and email
        # EmailPatternGuess, Score, Persist
