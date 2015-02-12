import requests
from bs4 import BeautifulSoup
import urllib
from webhook import Webhook
import pandas as pd
import tldextract
from parse import Parse
from google import Google
from li import Linkedin
import json
import re, string
import logging
from address import AddressParser, Address
import zoominfo
from email_guess import EmailGuess
from zoominfo import Zoominfo
import random
import toofr
from queue import RQueue
''' RQ Setup '''
from social import *
from rq import Queue
from worker import conn
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from company_db import *

q = Queue(connection=conn)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Companies:
    # TODO
    def _tags_describing_company():
        ''' '''

    def _description():
        ''' '''

    ''' Working '''
    def _company_blog(self, domain):
        ''' Find Company Blog '''
        # Google().search("site:{0} inurl:blog".format(domain))
        # get recent blog posts

    def _technologies(self, domain):
        ''' BuiltWith '''
        technology = requests.get('https://builtwith.com/'+domain)
        return technology

    def _traffic_analysis(self, domain):
        ''' Compete.com, Alexa, SimilarWeb '''
        traffic = requests.get('https://similarweb.com/'+domain)
        traffic = requests.get('https://alexa.com/siteinfo/'+domain)
        return traffic

    def _glassdoor(self, domain):
        ''' site:glassdoor.com/overview "cascadia metals" inurl:overview '''

    def _businessweek(self, domain):
        ''' '''

    def _forbes(self, domain):
        ''' http://www.forbes.com/companies/guidespark/ '''

    def _indeed_profile(self, domain):
        ''' Get Profile and Scrape Info'''

    def _hiring(self, company_name):
        # paginate
        jobs = "http://www.indeed.com/jobs?q={0}".format(company_name)
        return jobs

    def _social_reviews(self, company_name):
        ''' Glassdoor, Twitter, Facebook, Linkedin, GetApp '''
        return reviews

    def _press(self, company_name):
        ''' Google News, PRNewsWire, BusinessWire '''
        pw = Google().search("{0} site:prnewswire.com".format(company_name))
        bw = Google().search("{0} site:businesswire.com".format(company_name))
        # add marketwired, newswire.ca
        # parse and return
        #persist press

    def _news(self, company_name):
        gn = Google().news_search("{0}".format(company_name))
        # persist news

    def _social_profiles(self, domain):
        fb = Google().search("{0} site:facebook.com/".format(domain))
        tw = Google().search("{0} site:twitter.com/".format(domain))
        return social_profiles

    def _fundings(self, company_name):
        ''' Also find crunchbase handle '''
        results = Google().search("{0} site:crunchbase.com/organization".format(company_name))
        # scrape funding rounds
        return fundings

    def _employees(self, company_name, keyword=""):
        ''' Linkedin Scrape'''
        args = '-inurl:"/dir/" -inurl:"/find/" -inurl:"/updates"'
        args = args+' -inurl:"job" -inurl:"jobs2" -inurl:"company"'
        qry = '"at {0}" {1} {2} site:linkedin.com'
        qry = qry.format(company_name, args, keyword)
        results = Google().search(qry, 1)
        results = Linkedin()._google_df_to_linkedin_df(results)
        company_name = '(?i){0}'.format(company_name)
        results['company_score'] = [fuzz.ratio(company_name, company) 
                                    for company in results.company]
        results['score'] = [fuzz.ratio(keyword, title) 
                            for title in results.title]
        results = results[results.company_score > 84]
        results = results[results.score > 75]
        print results
        return results

    def _whois_info(self, domain):
        ''' Glean Info From Here'''

    def _crunchbase_profile(self, company_name):
        ''' Crunchbase Profile '''

    def _angellist_profile(self, company_name):
        ''' Angellist Profile '''

    def _related(self, domain):
        ''' Competitors, Similar Companies '''
        companies = Google().search("related:{0}".format(domain), 10)
        # linkedin companies info 
        return related

    def _bulk(self, company_name, api_key=""):
        qry = {'where':json.dumps({'company_name':company_name})}
        company = Parse().get('Company', qry).json()['results']
        print company
        if company:
            #Webhook()._post(api_key, company[0], 'company_info')
            Webhook()._update_company_info(company[0])
            return company[0]
        else:
            q.enqueue(Companies()._research, company_name, api_key)
            return {'Research has started.': True}

    def _research(self, name, api_key="", prospect_name=""):
        # Primary Research
        j9 = q.enqueue(Facebook()._company_profile, name, api_key,timeout=6000)
        j10 = q.enqueue(Twitter()._company_profile, name, api_key,timeout=6000)
        j11 = q.enqueue(Indeed()._company_profile, name, api_key,timeout=6000)
        jobs = [j9,j10,j11]
        '''
        j0 =q.enqueue(BusinessWeek()._company_profile, name, api_key,timeout=6000)
        j1 = q.enqueue(Zoominfo()._company_profile, name, api_key,timeout=6000)
        j2 = q.enqueue(Linkedin()._company_profile, name, api_key,timeout=6000)
        j3 = q.enqueue(YellowPages()._company_profile, name, api_key,timeout=6000)
        j4= q.enqueue(Yelp()._company_profile, name, api_key,timeout=6000)
        j5 = q.enqueue(Forbes()._company_profile, name, api_key,timeout=6000)
        j6 = q.enqueue(GlassDoor()._company_profile, name, api_key,timeout=6000)
        j7 = q.enqueue(Hoovers()._company_profile, name, api_key,timeout=6000)
        j8 = q.enqueue(Crunchbase()._company_profile, name, api_key,timeout=6000)
        jobs = [j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11]
        '''
        for job in jobs:
            RQueue()._meta(job, "{0}_{1}".format(name, api_key), prospect_name)
        # TODO - jigsaw
        # TODO - google plus

    def _domain_research(self, domain, api_key="", name="", prospect_name=""):
        # Primary Research
        if name == "": name=domain
        x = 6000
        j0 =q.enqueue(BusinessWeek()._domain_search, domain, api_key, name, timeout=x)
        j1 = q.enqueue(Zoominfo()._domain_search, domain, api_key, name, timeout=x)
        j2 = q.enqueue(Linkedin()._domain_search, domain, api_key,name,timeout=x)
        j3 = q.enqueue(YellowPages()._domain_search, domain, api_key,name,timeout=x)
        j4= q.enqueue(Yelp()._domain_search, domain, api_key, name,timeout=x)
        j5 = q.enqueue(Forbes()._domain_search, domain, api_key, name,timeout=x)
        j6 = q.enqueue(GlassDoor()._domain_search, domain, api_key, name,timeout=x)
        j7 = q.enqueue(Hoovers()._domain_search, domain, api_key, name,timeout=x)
        j8 = q.enqueue(Crunchbase()._domain_search, domain, api_key, name,timeout=x)
        j9 = q.enqueue(Facebook()._domain_search, domain, api_key, name,timeout=x)
        j10 = q.enqueue(Twitter()._domain_search, domain, api_key, name,timeout=x)
        j11 = q.enqueue(Indeed()._domain_search, domain, api_key, name,timeout=x)
        jobs = [j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11]
        for job in jobs:
            RQueue()._meta(job, "{0}_{1}".format(name, api_key), prospect_name)

    def _secondary_research(self, name, domain, api_key=""):
        # Secondary Research - sometimes require location or domain
        q.enqueue(Companies()._company_blog, domain)
        q.enqueue(Companies()._technologies, domain)
        q.enqueue(Glassdoor()._reviews, domain)
        q.enqueue(Crunchbase()._fundings, domain)
        q.enqueue(Companies()._press, company_name, domain)
        q.enqueue(Companies()._news, company_name, domain)
        q.enqueue(Companies()._hiring, domain)
        q.enqueue(Companies()._employees, company_name, domain)

        q.enqueue(Companies()._indeed_profile, domain)
        q.enqueue(Companies()._twitter, domain)
        q.enqueue(Companies()._facebook, domain)
        q.enqueue(Companies()._related, domain)
        #q.enqueue(Companies()._traffic_analysis, domain)
        #q.enqueue(Companies()._whois_info, domain)

    ''' In Use Methods '''
    def _email_pattern(self, domain):
        ''' ClearSpark '''
        patterns = EmailGuess().search_sources(domain)
        return patterns

    def _get_info(self, company_name, api_key=""):
        profile = Linkedin()._company_profile(company_name, api_key)
        if type(profile) is str: 
            profile = Zoominfo().search(company_name, api_key)
            logger.info("zoominfo company "+str(profile))
        #result = Parse()._add_company(profile, company_name)
        return profile

    def _get_long_info(self, company_name):
        profile = Linkedin()._company_profile(company_name)
        _profile = Zoominfo().search(company_name)
        print profile
        if type(profile) is str: 
            profile = Zoominfo().search(company_name)
            logger.info("zoominfo company "+str(profile))
        if type(profile) is not str and type(_profile) is not str: 
            profile['phone'] = _profile['phone']
        result = Parse()._add_company(profile, company_name)
        return profile

    def _async_get_info(self, company_name, update_object=False):
        profile = self._get_info(company_name)
        Parse().update('Prospect/'+str(update_object), profile, True).json()
        q.enqueue(EmailGuess().start_search, profile['domain'])

    def _get_info_webhook(self, company_name, objectId):
        profile = self._get_long_info(company_name)
        res = Parse().update('Prospect/'+objectId, profile, True).json()
        result = Parse().update('CompanyProspect/'+objectId, profile, True)
        return profile

class CompanyTrends:
    def linkedin_followers():
        ''' '''

    def alexa_rank():
        ''' '''

    def twitter_followers():
        ''' '''

    def angellist_followers():
        ''' '''

    def google_search_volume():
        ''' '''
