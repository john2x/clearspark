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

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class EmailGuess:
    def streaming_search(self, domain):
        google = Google()
        pw = google.search('"{0}" site:prnewswire.com'.format(domain))
        bw = google.search('"{0}" site:businesswire.com'.format(domain))

        for link in pw.link: 
            pn_emails = PRNewsWire()._find_emails(domain, link, False)
        for link in bw.link: 
            bw_emails = BusinessWire()._find_emails(domain, link, False)
        ''' enqueue and return values ''' 
        return pd.concat([pn_emails, bw_emails]).drop_duplicates('pattern')

    def start_search(self, domain):
        google = Google()
        pw = google.search('"{0}" site:prnewswire.com'.format(domain))
        bw = google.search('"{0}" site:businesswire.com'.format(domain))

        job_queue_lol = domain+str(arrow.now().timestamp)
        for link in pw.link: 
            print "STARTED", pw.shape
            job = q.enqueue(PRNewsWire()._find_emails, domain, link,
                            job_queue_lol, timeout=3600)
            job.meta['profile_id1'] = job_queue_lol
            job.save()

        for link in bw.link: 
            print "BW STARTED", bw.shape
            job = q.enqueue(BusinessWire._find_emails, domain, link,
                            job_queue_lol, timeout=3600)
            job.meta['profile_id1'] = job_queue_lol
            job.save()

    ''' Give Scores To Multiple Patterns '''
    def _score(self, patterns):
        print "_score"
        if patterns.shape[0] == 0:
            return patterns
        total = len(patterns.drop_duplicates().pattern)
        values = patterns.drop_duplicates('name').pattern.value_counts()
        upload = patterns.drop_duplicates('name')
        upload['instances'] = [i for i in patterns.email.value_counts()]
        upload['score'] = [int(float(i)/total*100) for i in values]
        return upload

    def _add_email_variables(self, contact_df):
        contacts['first_name'] = [name.split(' ')[0] for name in contacts.name]
        contacts['last_name'] = [name.split(' ')[-1] for name in contacts.name]
        contacts['first_initial'] = [name.split(' ')[0][0] for name in contacts.name]
        contacts['last_initial'] = [name.split(' ')[0][-1] for name in contacts.name]
        return contacts

    def _find_email_pattern(self, domain, results):
        ''' Decifer Email Pattern '''
        patterns = pd.DataFrame()
        for person in results:
            for pattern in self._patterns():
                email = pattern.format(**variables)
                if person['email'].lower() == email.lower():
                    info = [pattern.strip(), person['domain'].strip(), 
                            person['email'].lower().strip(), 
                            person['name'].title().strip()]
                    columns = ['pattern','domain','email','name']
                    patterns = patterns.append(dict(zip(columns, info)), 
                                               ignore_index=True)
        return patterns

    def _email_crawl_pointers(self, qry):
        crawls = pd.DataFrame(parse.get('CompanyEmailPatternCrawl', qry).json())
        crawl_objectids = crawls['results'].drop_duplicates('pattern').objectId
        crawl_pointers = [parse._pointer('CompanyEmailPatternCrawl', objectId)
                          for objectId in crawl_objectids]
        return crawl_pointers
    
    def _persist_email_guess(self, domain, upload):
        ''' Different Email Patterns '''
        if upload.shape[0] == 0: return 0
        for index, row in upload.iterrows():
            print Parse().create('CompanyEmailPatternCrawl', row.to_dict()).json()
        
        for domain in upload.domain.drop_duplicates():
            qry = {'where':json.dumps({'domain': domain}),'order':'-createdAt'}
            crawl_pointers = self._email_crawl_pointers(qry)
            patterns = {'company_email_pattern': crawl_pointers}
            pattern = Parse().get('CompanyEmailPattern', qry).json()

            if pattern['results'] == []:
                patterns['domain'] = domain
                print Parse().create('CompanyEmailPattern', patterns).json()
            else:
                pattern = 'CompanyEmailPattern/'+pattern['results'][0]['objectId']
                print Parse().update(pattern, patterns)

    def _patterns(self):
        return ['{first_name}@{domain}', '{last_name}@{domain}',
                '{first_initial}@{domain}', '{last_initial}@{domain}',
                '{first_name}{last_name}@{domain}',
                '{first_name}.{last_name}@{domain}',
                '{first_initial}{last_name}@{domain}',
                '{first_initial}.{last_name}@{domain}',
                '{first_name}{last_initial}@{domain}',
                '{first_name}.{last_initial}@{domain}',
                '{first_initial}{last_initial}@{domain}',
                '{first_initial}.{last_initial}@{domain}',
                '{last_name}{first_name}@{domain}',
                '{last_name}.{first_name}@{domain}',
                '{last_name}{first_initial}@{domain}',
                '{last_name}.{first_initial}@{domain}',
                '{last_initial}{first_name}@{domain}',
                '{last_initial}.{first_name}@{domain}',
                '{last_initial}{first_initial}@{domain}',
                '{last_initial}.{first_initial}@{domain}',
                '{first_name}-{last_name}@{domain}',
                '{first_initial}-{last_name}@{domain}',
                '{first_name}-{last_initial}@{domain}',
                '{first_initial}-{last_initial}@{domain}',
                '{last_name}-{first_name}@{domain}',
                '{last_name}-{first_initial}@{domain}',
                '{last_initial}-{first_name}@{domain}',
                '{last_initial}-{first_initial}@{domain}',
                '{first_name}_{last_name}@{domain}',
                '{first_initial}_{last_name}@{domain}',
                '{first_name}_{last_initial}@{domain}',
                '{first_initial}_{last_initial}@{domain}',
                '{last_name}_{first_name}@{domain}',
                '{last_name}_{first_initial}@{domain}',
                '{last_initial}_{first_name}@{domain}',
                '{last_initial}_{first_initial}@{domain}']

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
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        res         = EmailGuess()._find_email_pattern(domain, contacts)
        upload      = EmailGuess()._score(res)
        EmailGuess()._persist_email_guess(domain, upload)  

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            r = parse.get('CompanyEmailPattern', {'where': json.dumps({"domain":domain})})
            if r.json()['results'] == []:
                print "what is being printed?", domain, 'PRNewsWire'
                print r.json()
                vals = {'domain':domain, 'company_email_pattern': []}
                print parse.create('CompanyEmailPattern', vals)
        return upload

class BusinessWire:
    def _extract_contacts(self, html):
        contacts = BeautifulSoup(html)
        contacts = contacts.find('div',{'class':'bw-release-contact'})
        if contacts == None: return 0
        contacts = str(contacts).split('<br/>or<br/>')
        for contact in contacts:
            info = str(contact).split('<br/>')[1:]
            names = info[0].split(',')
            emails = [BeautifulSoup(i).text for i in info if "mailto:" in i]
        contacts = [{'name':name, 'email':email,'domain':email.split('@')[-1]} 
                    for name, email in zip(names, emails)]
        return pd.DataFrame(contacts)

    def _find_emails(domain, link, job_queue_lol):
        ''' BusinessWire '''
        parse, html = Parse(), requests.get(link).text
        contacts    = BusinessWire()._extract_contacts(html)
        res         = EmailGuess()._find_email_pattern(domain, contacts)
        upload      = EmailGuess()._score(res)
        EmailGuess()._persist_email_guess(domain, upload)  

        if QueueHelper()._is_done(job_queue_lol) and job_queue_lol:
            r = parse.get('CompanyEmailPattern', {'where': json.dumps({'domain':domain})})
            if r.json()['results'] == []:
                print "what is being printed?", domain, 'BusinessWire'
                print r.json()
                vals = {'domain':domain, 'company_email_pattern':"not found"}
                print parse.create('CompanyEmailPattern', vals)
        return upload


class QueueHelper:
    def _is_done(self, profile_id):
        profile_jobs = [job.meta for job in q.jobs 
                        if 'profile_id1' in job.meta.keys()]
        last_one     = [job for job in profile_jobs 
                        if job['profile_id1'] == profile_id]
        print "NUMBER OF JOBS", len(last_one)
        return len(last_one) == 0


