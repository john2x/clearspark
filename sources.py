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
    contact = BeautifulSoup(r.text).find('div',{'class':'bw-release-contact'})
    info = [person.split(',')
            for person in str(contact).split('<br/>') 
            if "mailto:" in person]
    names = [" ".join(name[0].split()) for name in info]
    emails = [BeautifulSoup(name[1]).text for name in info]
    results = [{'name':name, 'email':email,'domain':domain} 
               for name, email in zip(names, emails)]
    res = email_patterns._decifer(results)
    upload = email_patterns._score(res)
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
                       
    results = [{'name':name, 'email':email, 'domain':domain} 
               for name, email in zip(names, emails)]
    res = email_patterns._decifer(results)
    upload = email_patterns._score(res)
    email_patterns._persist(domain, upload)  
    if _queue_is_done(job_queue_lol):
        r = parse.get('CompanyEmailPattern', {'domain':domain}).json()
        if r['results'] == []:
            vals = {'domain':domain, 'company_email_pattern':[]}
            parse.create('CompanyEmailPattern', vals)

def _queue_is_done(profile_id):
    print len(q.jobs)
    profile_jobs = [job.meta for job in q.jobs 
                             if 'profile_id1' in job.meta.keys()]
    last_one = [job for job in profile_jobs 
                    if job['profile_id1'] == profile_id]

    print "NUMBER OF JOBS", last_one
    return len(last_one) == 0
