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

    def _add_email_variables(self, contacts):
        #print contacts
        try:
          contacts['first_name'] = [name.split(' ')[0] for name in contacts.name]
        except:
          print contacts
        contacts['last_name'] = [name.split(' ')[-1] for name in contacts.name]
        contacts['first_initial'] = [name.split(' ')[0][0] for name in contacts.name]
        contacts['last_initial'] = [name.split(' ')[0][-1] for name in contacts.name]
        return contacts

    def _find_email_pattern(self, domain, results):
        ''' Decifer Email Pattern '''
        patterns = pd.DataFrame()
        for index, person in results.iterrows():
            for pattern in self._patterns():
                email = pattern.format(**person)
                if person['email'].lower() == email.lower():
                    info = [pattern.strip(), person['domain'].strip(), 
                            person['email'].lower().strip(), 
                            person['name'].title().strip()]
                    columns = ['pattern','domain','email','name']
                    patterns = patterns.append(dict(zip(columns, info)), 
                                               ignore_index=True)
        return patterns

    def _email_crawl_pointers(self, qry):
        parse = Parse()
        results = parse.get('CompanyEmailPatternCrawl', qry).json()
        results = results['results'] if "results" in results.keys() else results
        crawls = pd.DataFrame(results)
        print crawls
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
