from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
import tldextract
from parse import Parse

class CompanyInfoCrawl:
    def _persist(self, data, source=""):
        crawl = Parse().create('CompanyInfoCrawl', data).json()
        crawl = Parse()._pointer('CompanyInfoCrawl', crawl['objectId'])
        company = Parse().get('Company', {'where':json.dumps({'domain':data['domain']})}).json()
        data['crawl_source'] = [source for i in range(data.shape[0])]
        print company
        if company['results']:
            company = 'Company/'+company['results'][0]['objectId'], 
            data = {'__op':'AddUnique', "objects":[crawl]}
            print "update", Parse().update(company, {'crawls': data}).json()
        else:
            print Parse().create(company, {'crawls':[crawl],'domain':data['domain']}).json()

class CompanyEmailPatternCrawl:
    def _bulk_persist(self, data):
        ''' '''

    def _persist(self, data, source=""):
        print source, data, "PERSISTING YOYO"
        data['crawl_source'] = source
        for index, row in data.iterrows():
            print row.to_dict()
            r  = Parse().create('CompanyEmailPatternCrawl', row.to_dict()).json()
            print r
