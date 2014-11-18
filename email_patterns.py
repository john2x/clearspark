import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import json

''' Decifer Email Pattern '''
def _decifer(results):
    patterns = pd.DataFrame()
    for person in results:
        variables = {'first_name': person['name'].split(' ')[0],
                     'last_name' : person['name'].split(' ')[-1],
                     'first_initial': person['name'].split(' ')[0][0],
                     'last_initial': person['name'].split(' ')[-1][0],
                     'domain': person['domain']}
        for pattern in email_patterns():
            email = pattern.format(**variables)
            if person['email'].lower() == email.lower():
                info = [pattern, domain, person['email'].lower(), person['name'].title()]
                columns = ['pattern','domain','email','name']
                patterns = patterns.append(dict(zip(columns, info)),ignore_index=True)
                
    return patterns

''' Give Scores To Multiple Patterns '''
def _score(patterns):
    total = len(patterns.drop_duplicates().pattern)
    values = patterns.drop_duplicates('name').pattern.value_counts()
    upload = patterns.drop_duplicates('name')
    upload['instances'] = [i for i in patterns.email.value_counts()]
    upload['score'] = [int(float(i)/total*100) for i in values]
    return upload
    
def _persist(upload):
    ''' Different Email Patterns '''
    parse = Parse()
    pointers = []
    for index, row in upload.iterrows():
        r = parse.create('CompanyEmailPatternCrawl', row.to_dict()).json()
    
    for domain in upload.domain.drop_duplicates():
        qry = json.dumps({'domain': domain})
        pattern = parse.get('CompanyEmailPattern',{'where':qry}).json()
        crawls = parse.get('CompanyEmailPatternCrawl',{'where':qry,'order':'-createdAt'}).json()
        crawls = pd.DataFrame(crawls['results']).drop_duplicates('pattern')
        pointers = [parse._pointer('CompanyEmailPatternCrawl', objectId)
                    for objectId in crawls['objectId']]
        patterns = {'company_email_pattern':pointers}
        print pattern, patterns
        if pattern['results'] == []:
            patterns['domain'] = domain
            r = parse.create('CompanyEmailPattern', patterns)
            print r.json()
        else:
            pattern = pattern['results'][0]
            print parse.get('CompanyEmailPattern/'+pattern['objectId']).json()
            r = parse.update('CompanyEmailPattern/'+pattern['objectId'],patterns)
            print r.json()
