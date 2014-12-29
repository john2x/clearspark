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
        #results = results[results.company.str.contains(company_name)]
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

    def _research(self, company_name):
        # Primary Research - [scored]
        q.enqueue(Zoominfo()._company_profile, company_name)
        q.enqueue(Linkedin()._company_profile, company_name)
        q.enqueue(YellowPages()._company_profile, company_name)
        q.enqueue(Yelp()._company_profile, company_name)
        #q.enqueue(Companies()._businessweek, company_name)
        #q.enqueue(Companies()._forbes, company_name)
        #q.enqueue(Companies()._crunchbase_profile, company_name)

        # Secondary Research - sometimes require location or domain
        #q.enqueue(Companies()._company_blog, domain)
        #q.enqueue(Companies()._technologies, domain)
        #q.enqueue(Companies()._traffic_analysis, domain)
        #q.enqueue(Companies()._glassdoor, domain)
        #q.enqueue(Companies()._twitter, domain)
        #q.enqueue(Companies()._facebook, domain)
        #q.enqueue(Companies()._indeed_profile, domain)
        #q.enqueue(Companies()._hiring, domain)
        #q.enqueue(Companies()._fundings, domain)
        #q.enqueue(Companies()._related, domain)
        #q.enqueue(Companies()._whois_info, domain)

        #q.enqueue(Companies()._press, company_name)
        #q.enqueue(Companies()._news, company_name)
        #q.enqueue(Companies()._employees, company_name)
        '''
        q.enqueue(Facebook()._company_profile(company_name))
        q.enqueue(Twitter()._company_profile(company_name))
        '''

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
