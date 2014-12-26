import requests
from bs4 import BeautifulSoup
import urllib
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
''' RQ Setup '''
from rq import Queue
from worker import conn
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from social import Yelp
from social import YellowPages
q = Queue(connection=conn)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Companies:
    def _company_blog():
        ''' Find Company Blog '''
        
    def _technologies(self, domain):
        ''' BuiltWith '''
        technology = requests.get('https://builtwith.com/'+domain)
        return technology

    def _traffic_analysis(self, domain):
        ''' Compete.com, Alexa, SimilarWeb '''
        traffic = requests.get('https://similarweb.com/'+domain)
        traffic = requests.get('https://alexa.com/siteinfo/'+domain)
        return traffic

    def _hiring(self, company_name):
        ''' Also add Indeed profile'''
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
        gn = Google().news_search("{0}".format(company_name))
        # parse and return
        return  press

    def _social_profiles(self, domain):
        ''' Clearbit '''
        fb = Google().search("{0} site:facebook.com/".format(domain))
        tw = Google().search("{0} site:twitter.com/".format(domain))
        return social_profiles

    def _fundings(self, company_name):
        ''' Also find crunchbase profile Crunchbase '''
        results = Google().search("{0} site:crunchbase.com".format(company_name))
        # scrape
        return fundings

    def _employee_estimate():
        ''' '''

    def _employees(self, company_name, keyword=""):
        ''' Linkedin Scrape'''
        args = '-inurl:"/dir/" -inurl:"/find/" -inurl:"/updates"'
        args = args+' -inurl:"job" -inurl:"jobs2" -inurl:"company"'
        qry = '"at {0}" {1} {2} site:linkedin.com'
        qry = qry.format(company_name, args, keyword)
        results = Google().search(qry, 1)
        results = Linkedin()._google_df_to_linkedin_df(results)
        company_name = '(?i){0}'.format(company_name)
        #results = results[results.company.str.contains(company_name)]
        results['company_score'] = [fuzz.ratio(company_name, company) 
                                    for company in results.company]
        results['score'] = [fuzz.ratio(keyword, title) 
                            for title in results.title]
        results = results[results.company_score > 84]
        results = results[results.score > 75]
        print results
        # rename fields - name, title, company
        return results

    def _related(self, domain):
        ''' Competitors, Similar Companies '''
        companies = Google().search("related:{0}".format(domain), 10)
        # linkedin companies info 
        return related

    def _tags_describing_company():
        ''' '''

    def _description():
        ''' '''

    ''' In Use Methods '''
    def _email_pattern(self, domain):
        ''' ClearSpark '''
        patterns = EmailGuess().streaming_search(domain)
        return patterns

    def _get_info(self, company_name):
        profile = Linkedin()._company_profile(company_name)
        if type(profile) is str: 
            profile = Zoominfo().search(company_name)
            logger.info("zoominfo company "+str(profile))
        result = Parse()._add_company(profile, company_name)
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

    def _research(self, company_name):
        ''' Research '''
        q.enqueue(Zoominfo()._company_profile, company_name)
        q.enqueue(Linkedin()._company_profile, company_name)
        q.enqueue(YellowPages()._company_profile, company_name)
        q.enqueue(Yelp()._company_profile, company_name)
        # sometimes require location or domain
        '''
        q.enqueue(Facebook()._company_profile(company_name))
        q.enqueue(Twitter()._company_profile(company_name))
        '''

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
