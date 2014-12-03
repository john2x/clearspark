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
from email_guess_helper import EmailGuessHelper
from nameparser import HumanName
import pandas as pd
import json

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class PRNewsWire:
    ''' '''
    def _extract_contacts(self, html):
        names, emails, contact = [], [], BeautifulSoup(html)
        for paragraph in contact.findAll('p',attrs={"itemprop" : "articleBody"}):
            people = paragraph.findAll('span', {'class':'xn-person'})
            names = [name.text for name in people]
            emails = [email.text for email in paragraph.findAll('a') 
                           if "mailto:" in email['href']]
                           
        results = [{'name':name, 'email':email, 'domain':email.split('@')[-1]} 
                   for name, email in zip(names, emails)]
        return pd.DataFrame(results)

    def _find_emails(self, domain, link, job_queue_lol):
        ''' PR Newswire '''
        print "PRNewsWire"
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        if not contacts.empty: 
            contacts    = EmailGuessHelper()._add_email_variables(html)
            res         = EmailGuessHelper()._find_email_pattern(domain, contacts)
            upload      = EmailGuessHelper()._score(res)
            EmailGuessHelper()._persist_email_guess(domain, upload)  
        else:
            print "no prospects found"

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            r = parse.get('CompanyEmailPattern', 
                          {'where': json.dumps({"domain":domain})})
            if r.json()['results'] == []:
                print "what is being printed?", domain, 'PRNewsWire'
                print r.json()
                vals = {'domain':domain, 'company_email_pattern': []}
                print parse.create('CompanyEmailPattern', vals)
        #return upload

    def _email_webhook(self, domain, link, job_queue_lol, objectId):
        ''' PR Newswire '''
        print "PRNewsWire"
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        contact = {}
        if not contacts.empty: 
            contacts    = EmailGuessHelper()._add_email_variables(html)
            contacts    = EmailGuessHelper()._find_email_pattern(domain, contacts)
            contacts    = EmailGuessHelper()._score(res)
            contacts    = contacts[contacts.domain == domain]
            contacts    = contacts.drop_duplicates('domain')
            contact     = contacts.ix[0].to_dict()
        else:
            print "no prospects found"

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            print "Final Contact", contact
            print Parse().update('Prospect/'+objectId, contact).json()

class BusinessWire:
    def _extract_contacts(self, html):
        contacts = BeautifulSoup(html)
        contacts = contacts.find('div',{'class':'bw-release-contact'})
        if contacts == None: return pd.DataFrame()
        contacts = str(contacts).split('<br/>or<br/>')
        for contact in contacts:
            info = str(contact).split('<br/>')[1:]
            names = info[0].split(',')
            emails = [BeautifulSoup(i).text for i in info if "mailto:" in i]
        contacts = [{'name':name, 'email':email,'domain':email.split('@')[-1]} 
                    for name, email in zip(names, emails)]
        return pd.DataFrame(contacts)

    def _find_emails(self, domain, link, job_queue_lol):
        ''' BusinessWire '''
        print "BusinessWire"
        parse, html, upload = Parse(), requests.get(link).text, ""
        contacts    = BusinessWire()._extract_contacts(html)
        if not contacts.empty: 
            contacts    = EmailGuessHelper()._add_email_variables(contacts)
            res         = EmailGuessHelper()._find_email_pattern(domain, contacts)
            upload      = EmailGuessHelper()._score(res)
            EmailGuessHelper()._persist_email_guess(domain, upload)  
        else:
            print "no prospects found"

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            r = parse.get('CompanyEmailPattern', {'where': json.dumps({'domain':domain})})
            if r.json()['results'] == []:
                print "what is being printed?", domain, 'BusinessWire'
                print r.json()
                vals = {'domain':domain, 'company_email_pattern':"not found"}
                print parse.create('CompanyEmailPattern', vals)
        return upload

    def _email_webhook(self, domain, link, job_queue_lol, objectId):
        ''' BusinessWire '''
        print "BusinessWire"
        parse, html, upload = Parse(), requests.get(link).text, ""
        contacts    = BusinessWire()._extract_contacts(html)
        contact = {}
        if not contacts.empty: 
            contacts    = EmailGuessHelper()._add_email_variables(contacts)
            contacts    = EmailGuessHelper()._find_email_pattern(domain, contacts)
            contacts    = EmailGuessHelper()._score(res)
            contacts    = contacts[contacts.domain == domain]
            contacts    = contacts.drop_duplicates('domain')
            contact     = contacts.ix[0].to_dict()
            #EmailGuessHelper()._persist_email_guess(domain, upload)  
        else:
            print "no prospects found"

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            print "FINAL CONTACT", contact
            print Parse().update('Prospect/'+objectId, contact).json()


class QueueHelper:
    def _is_done(self, profile_id):
        profile_jobs = [job.meta for job in q.jobs 
                        if 'profile_id1' in job.meta.keys()]
        last_one     = [job for job in profile_jobs 
                        if job['profile_id1'] == profile_id]
        print "NUMBER OF JOBS", len(last_one)
        return len(last_one) == 0


