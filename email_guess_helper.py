import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import arrow
import json

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class EmailGuessHelper:
    def _remove_non_ascii(self, text):
          return ''.join(i for i in text if ord(i)<128)

    def _name_to_email_variables(self, name):
        name = self._remove_non_ascii(name.strip())
        fi = name.split(' ')[0][0]
        li = name.split(' ')[-1][0]
        first = name.split(' ')[0]
        last = name.split(' ')[-1]
        vals = [fi, li, first, last, name]
        cols = ['first_initial','last_initial','first_name','last_name','name']
        return dict(zip(cols,vals))

    def _find_email_pattern(self, name, email):
        ''' Decifer Email Pattern '''
        patterns = pd.DataFrame()
        print name, email, 'find_email_pattern'
        person = self._name_to_email_variables(str(name).strip())
        person['domain'] = email.strip().split('@')[-1]
        for pattern in self._patterns():
            _email = pattern.format(**person)
            print _email.lower(), email.lower(), _email.lower() == email.lower()
            if email.lower() != _email.lower(): continue
            info = [pattern.strip(), person['domain'].strip(), 
                    person['email'].lower().strip(), person['name'].title().strip()]
            columns = ['pattern','domain','email','name']
            patterns = patterns.append(dict(zip(columns, info)), ignore_index=True)
        return patterns

    def _bulk_find_email_pattern(self, domain, people):
        ''' Decifer Email Pattern '''
        patterns = pd.DataFrame()
        for index, person in people.iterrows():
            for pattern in self._patterns():
                email = pattern.format(**person)
                if person['email'].lower() != email.lower(): continue
                info = [pattern.strip(), 
                        person['domain'].strip(), 
                        person['email'].lower().strip(), 
                        person['name'].title().strip()]
                columns = ['pattern','domain','email','name']
                patterns = patterns.append(dict(zip(columns, info)), 
                                            ignore_index=True)
        return patterns

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

    def _email_crawl_pointers(self, qry):
        parse = Parse()
        results = parse.get('CompanyEmailPatternCrawl', qry).json()
        results = results['results'] if "results" in results.keys() else results
        crawls = pd.DataFrame(results)
        crawl_objectids = crawls.drop_duplicates('pattern').objectId
        crawl_pointers = [parse._pointer('CompanyEmailPatternCrawl', objectId)
                          for objectId in crawl_objectids]
        return crawl_pointers

    def _update_prospect_with_email(self, contact, objectId):
        print Parse().update('Prospect/'+objectId, contact).json()
        print r.json()
        ''' '''

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

    def _add_email_variables(self, contacts):
        contacts['first_name'] = [self._remove_non_ascii(name).strip().split(' ')[0] 
                                    for name in contacts.name]
        contacts['last_name'] = [self._remove_non_ascii(name).split(' ')[-1] 
                                 for name in contacts.name]
        contacts['first_initial'] = [self._remove_non_ascii(name).split(' ')[0][0] 
                                     for name in contacts.name]
        contacts['last_initial'] = [self._remove_non_ascii(name).split(' ')[0][-1] 
                                    for name in contacts.name]
        return contacts
