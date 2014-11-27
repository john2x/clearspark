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
from address import AddressParser, Address
import zoominfo
from email_patterns import EmailGuess
from zoominfo import Zoominfo
import random
import toofr
''' RQ Setup '''
from rq import Queue
from worker import conn
q = Queue(connection=conn)

class Companies:
    def _technologies(self, domain):
        ''' BuiltWith '''
        technology = requests.get('https://builtwith.com/'+domain)
        return technology

    def _traffic_analysis(self, domain):
        ''' Compete.com, Alexa, SimilarWeb '''
        traffic = requests.get('https://similarweb.com/'+domain)
        return traffic

    def _hiring(self, company_name):
        ''' Indeed '''
        jobs = "http://www.indeed.com/jobs?q={0}".format(company_name)
        return jobs

    def _social_reviews(self, company_name):
        ''' Glassdoor, Twitter, Facebook, Linkedin, GetApp'''
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
        ''' Crunchbase '''
        results = Google().search("{0} site:crunchbase.com".format(company_name))
        # scrape
        return fundings

    def _employees(self, company_name, keyword=""):
        ''' Linkedin Scrape'''
        args = '-inurl:"/dir/" -inurl:"/find/" -inurl:"/updates"'
        qry = '"at {0}" {1} site:linkedin.com'.format(company_name, args)
        results = Google().search(qry, 10)
        # filter companies with company_name
        return employees

    def _related(self, domain):
        ''' Competitors, Similar Companies '''
        companies = Google().search("related:{0}".format(domain), 10)
        # linkedin companies info 
        return related

    ''' In Use Methods '''

    def _email_pattern(self, domain):
        ''' ClearSpark '''
        patterns = EmailGuess().streaming_search(domain)
        return patterns

    def _get_info(self, company_name):
        profile = Linkedin()._company_profile(company_name)
        print profile
        if str(profile) is "not found":
            profile = Zoominfo().search(company_name)

        if str(profile) != "not found":
            q.enqueue(Parse()._add_company, profile.ix[0].to_dict(), 
                      company_name, timeout=3600)
        return profile

    def _async_get_info(self, company_name, update_object=False):
        profile = Linkedin()._company_profile(company_name)
        print profile
        if str(profile) is "not found":
            profile = Zoominfo().search(company_name)
        if str(profile) != "not found":
            q.enqueue(Parse()._add_company, profile.ix[0].to_dict(), 
                      company_name, timeout=3600)

        if str(profile) != "not found" and update_object:
            profile = profile.ix[0].to_dict()
            print profile
            r = Parse().update('Prospect/'+update_object, profile, True).json()
            print r
            '''
            if 'domain' in profile.keys():
                q.enqueue(EmailGuess().start_search, profile.ix[0].to_dict()['domain'])
            '''

    def search(self, company_name):
        print "Started", company_name
        profile = self._get_info(company_name)
        if profile is "not found": return "not found"
        #profile['pattern'] = self._email_pattern(profile['domain'])
        print profile
        return profile

    def async_search(self, company_name):
        profile = self._async_get_info(company_name)
        if profile is "not found": return "not found"
        # persist
        return profile
