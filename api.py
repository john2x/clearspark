from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
import json
import tldextract
from parse import Parse
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

from rq import Queue
from worker import conn
q = Queue(connection=conn)

app = FlaskAPI(__name__)
@app.route('/v1/companies/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def find_email_address():
    parse, google = Parse(), Google()
    domain = request.args['domain']
    # tld extract
    qry = json.dumps({'domain': domain})
    qry = {'where':qry, 'include':'company_email_pattern'}
    pattern = parse.get('CompanyEmailPattern', qry)
    pw = google.search('"{0}" site:prnewswire.com'.format())
    bw = google.search('"{0}" site:businesswire.com'.format('fastspring.com'))

    for link in pw.link: q.enqueue(prnewwire_google_search, domain)
    for link in bw.link: q.enqueue(businesswire_google_search, domain)

    if r.json() is []:
      return {'queued': True}
    else:
      return pattern

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
        email_patterns._persist(upload)  

def prnewswire_google_search(domain):
    ''' PR Newswire '''
    parse, google = Parse(), Google()
    pw = google.search('"{0}" site:prnewswire.com/news'.format(domain))
    for link in pw.link:
        r = requests.get(link)
        contact = BeautifulSoup(r.text)
        
        for paragraph in contact.findAll('p',attrs={"itemprop" : "articleBody"}):
            names = [name.text for name in paragraph.findAll('span', {'class':'xn-person'})]
            emails = [email.text for email in paragraph.findAll('a') 
                           if "mailto:" in email['href']]
                           
        results = [{'name':name, 'email':email, 'domain':domain} 
                   for name, email in zip(names, emails)]
        res = email_patterns._decifer(results)
        upload = email_patterns._score(res)
        email_patterns._persist(upload)  


@app.route('/v1/companies/streaming/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def search(''):
    # search google for business wire
    ''' '''

@app.route('/', methods=['GET'])
def test():
    return {"test": "lol"}

@app.route('/hirefire/c15c66e82a043712ea5ed5f6f67835ec33c2da37/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def get_job_count():
    #return {"job count": len(q.jobs)}
      return [{"name": "worker", "quantity" : len(q.jobs)}]
