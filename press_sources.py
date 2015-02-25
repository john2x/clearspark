from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
import json
import tldextract
from parse import Parse
import logging
from crossdomain import crossdomain
from google import Google
import toofr
import requests
import arrow
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from email_guess_helper import EmailGuessHelper
from nameparser import HumanName
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime
import json
from crawl import CompanyEmailPatternCrawl

from rq import Queue
from worker import conn
q = Queue(connection=conn)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PRNewsWire:
    def _recent_to_dict():
        ''' '''

    def _article_to_dict():
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
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        if contacts.empty: return contacts
        contacts = EmailGuessHelper()._add_email_variables(contacts)
        contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        CompanyEmailPatternCrawl()._persist(contacts)

    def _email(self, domain, link):
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        if not contacts.empty: 
            logger.info(contacts)
            contacts = contacts[contacts.domain == domain]
            contacts = contacts.drop_duplicates('domain')
            contacts = EmailGuessHelper()._add_email_variables(contacts)
            contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        CompanyEmailPatternCrawl()._persist(contacts)
        return contacts

    def _email_webhook(self, domain, link, job_queue_lol, objectId):
        parse, html = Parse(), requests.get(link).text
        contacts    = self._extract_contacts(html)
        if not contacts.empty: 
            logger.info(contacts)
            contacts = contacts[contacts.domain == domain]
            contacts = contacts.drop_duplicates('domain')
            contacts = EmailGuessHelper()._add_email_variables(contacts)
            contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        else:
            print "no prospects found"
        CompanyEmailPatternCrawl()._persist(contacts)

class BusinessWire:
    def _recent_to_dict():
      ''' '''

    def _article_to_dict():
      ''' '''

    def _extract_contacts(self, html):
        contacts = BeautifulSoup(html)
        contacts = contacts.find('div',{'class': 'bw-release-contact'})
        if contacts == None: return pd.DataFrame()
        contacts = str(contacts).split('<br/>or<br/>')
        for contact in contacts:
            info = str(contact).split('<br/>')[1:]
            names = info[0].split(',')
            emails = [BeautifulSoup(i).text for i in info if "mailto:" in i]
        contacts = [{'name':name, 'email':email,'domain':email.split('@')[-1]} 
                    for name, email in zip(names, emails)]
        return pd.DataFrame(contacts)

    def _find_emails(self, domain, link, job_queue_lol):
        parse, html, upload = Parse(), requests.get(link).text, ""
        contacts    = BusinessWire()._extract_contacts(html)
        if not contacts.empty: 
            contacts = EmailGuessHelper()._add_email_variables(contacts)
            contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        else:
            print "no prospects found"
        CompanyEmailPatternCrawl()._persist(contacts)
        return upload

    def _email(self, domain, link):
        parse, html, upload = Parse(), requests.get(link).text, ""
        contacts    = BusinessWire()._extract_contacts(html)
        if not contacts.empty: 
            logger.info(contacts)
            contacts = contacts[contacts.domain == domain]
            contacts = contacts.drop_duplicates('domain')
            contacts = EmailGuessHelper()._add_email_variables(contacts)
            contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        CompanyEmailPatternCrawl()._persist(contacts)
        return contacts

    def _email_webhook(self, domain, link, job_queue_lol, objectId):
        ''' BusinessWire '''
        print "BusinessWire"
        parse, html, upload = Parse(), requests.get(link).text, ""
        contacts    = BusinessWire()._extract_contacts(html)
        if contacts.empty: return contacts
        logger.info(contacts)
        contacts = contacts[contacts.domain == domain]
        contacts = contacts.drop_duplicates('domain')
        contacts = EmailGuessHelper()._add_email_variables(contacts)
        contacts = EmailGuessHelper()._bulk_find_email_pattern(domain, contacts)
        CompanyEmailPatternCrawl()._persist(contacts)

class NewsWire:
    def _recent_to_dict():
      ''' '''

    def _article_to_dict():
      ''' '''

class MarketWire:
    def _recent_to_dict():
      ''' '''

    def _article_to_dict():
      ''' '''


