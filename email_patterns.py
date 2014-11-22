import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import json

class EmailGuess:
    def streaming_search(self, domain):
        google = Google()
        pw = google.search('"{0}" site:prnewswire.com'.format(domain))
        bw = google.search('"{0}" site:businesswire.com'.format(domain))

        for link in pw.link: 
            pn_emails = PRNewsWire()._find_emails(domain, link, False)
        for link in bw.link: 
            bw_emails = BusinessWire(domain, link, False)
        ''' enqueue and return values ''' 
        return pd.concat([pn_emails, bw_emails]).drop_duplicates('pattern')

    def start_search(self, domain):
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
            for pattern in _patterns():
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
