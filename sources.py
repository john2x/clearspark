from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
import json
import tldextract
from parse import Parse
from crossdomain import crossdomain
from google import Google
import toofr
import requests
import arrow
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import json
import email_patterns

from rq import Queue
from worker import conn
q = Queue(connection=conn)

def businesswire_google_search(domain, link, job_queue_lol):
    ''' BusinessWire '''
    parse = Parse()
    r = requests.get(link)
    contacts = BeautifulSoup(r.text).find('div',{'class':'bw-release-contact'})
    if contacts == None: return 0
    contacts = str(contacts).split('<br/>or<br/>')
    for contact in contacts:
        info = str(contact).split('<br/>')[1:]
        names = info[0].split(',')
        emails = [BeautifulSoup(i).text for i in info if "mailto:" in i]
    results = [{'name':name, 'email':email,'domain':email.split('@')[-1]} 
               for name, email in zip(names, emails)]
    res = email_patterns._decifer(domain, results)
    upload = email_patterns._score(res)
    print upload
    email_patterns._persist(domain, upload)  

    if _queue_is_done(job_queue_lol):
        r = parse.get('CompanyEmailPattern', {'domain':domain}).json()
        if r['results'] == []:
            vals = {'domain':domain, 'company_email_pattern':[]}
            parse.create('CompanyEmailPattern', vals)

def prnewswire_google_search(domain, link, job_queue_lol):
    ''' PR Newswire '''
    parse = Parse()
    r = requests.get(link)
    contact = BeautifulSoup(r.text)
    
    names, emails = [], []
    for paragraph in contact.findAll('p',attrs={"itemprop" : "articleBody"}):
        names = [name.text for name in paragraph.findAll('span', {'class':'xn-person'})]
        emails = [email.text for email in paragraph.findAll('a') 
                       if "mailto:" in email['href']]
                       
    results = [{'name':name, 'email':email, 'domain':email.split('@')[-1]} 
               for name, email in zip(names, emails)]
    res = email_patterns._decifer(domain, results)
    upload = email_patterns._score(res)
    print upload
    email_patterns._persist(domain, upload)  
    if _queue_is_done(job_queue_lol):
        r = parse.get('CompanyEmailPattern', {'domain':domain}).json()
        if r['results'] == []:
            print "Empty"
            vals = {'domain':domain, 'company_email_pattern':[]}
            parse.create('CompanyEmailPattern', vals)

def _queue_is_done(profile_id):
    profile_jobs = [job.meta for job in q.jobs 
                             if 'profile_id1' in job.meta.keys()]
    last_one = [job for job in profile_jobs 
                    if job['profile_id1'] == profile_id]

    print "NUMBER OF JOBS", len(last_one)
    return len(last_one) == 0

def start_search(domain):
    google = Google()
    pw = google.search('"{0}" site:prnewswire.com'.format(domain))
    bw = google.search('"{0}" site:businesswire.com'.format(domain))

    job_queue_lol = domain+str(arrow.now().timestamp)
    for link in pw.link: 
        print "STARTED", pw.shape
        job = q.enqueue(prnewswire_google_search, domain, link,
                        job_queue_lol, timeout=3600)
        job.meta['profile_id1'] = job_queue_lol
        job.save()
    for link in bw.link: 
        print "BW STARTED", bw.shape
        job = q.enqueue(businesswire_google_search, domain, link,
                        job_queue_lol, timeout=3600)
        job.meta['profile_id1'] = job_queue_lol
        job.save()


