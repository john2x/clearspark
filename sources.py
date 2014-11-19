from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
import json
import tldextract
from parse import Parse
from crossdomain import crossdomain
from google import Google
import toofr
import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
import pandas as pd
import json
import email_patterns

def businesswire_google_search(domain):
    ''' BusinessWire '''
    parse, google = Parse(), Google()
    bw = google.search('"{0}" site:businesswire.com/news'.format(domain))
    for link in bw.link:
        r = requests.get(link)
        contact = BeautifulSoup(r.text).find('div',{'class':'bw-release-contact'})
        info = [person.split(',')
                for person in str(contact).split('<br/>') 
                if "mailto:" in person]
        names = [" ".join(name[0].split()) for name in info]
        emails = [BeautifulSoup(name[1]).text for name in info]
        results = [{'name':name, 'email':email,'domain':domain} 
                   for name, email in zip(names, emails)]
        res = email_patterns._decifer(results)
        upload = email_patterns._score(res)
        email_patterns._persist(domain, upload)  

def prnewswire_google_search(domain):
    ''' PR Newswire '''
    parse, google = Parse(), Google()
    pw = google.search('"{0}" site:prnewswire.com/news'.format(domain))
    for link in pw.link:
        r = requests.get(link)
        contact = BeautifulSoup(r.text)
        
        names, emails = [], []
        for paragraph in contact.findAll('p',attrs={"itemprop" : "articleBody"}):
            names = [name.text for name in paragraph.findAll('span', {'class':'xn-person'})]
            emails = [email.text for email in paragraph.findAll('a') 
                           if "mailto:" in email['href']]
                           
        results = [{'name':name, 'email':email, 'domain':domain} 
                   for name, email in zip(names, emails)]
        res = email_patterns._decifer(results)
        upload = email_patterns._score(res)
        email_patterns._persist(domain, upload)  
