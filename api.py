from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
from sources import Sources
import json
import tldextract
import arrow
from parse import Parse
import whois
from crossdomain import crossdomain
from google import Google
import toofr
import requests
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
from press_sources import PRNewsWire
from press_sources import BusinessWire
from email_guess import EmailGuess
from companies import Companies
from company_score import CompanyScore
import pandas as pd
import json
import email_guess
from sources import Sources
from mining_job import MiningJob
from score import Score
from webhook import Webhook
import unicodedata
import os
from company_db import *
from jigsaw import Jigsaw
import bugsnag
from bugsnag.flask import handle_exceptions

from rq import Queue
from worker import conn
q = Queue(connection=conn)

app = FlaskAPI(__name__)
bugsnag.configure(
  api_key = "2556d33391f9accc8ea79325cd78ab62",
)

handle_exceptions(app)
  
@app.route('/v1/jigsaw_search', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def jigsaw_search():
    company_name = request.args['company_name']
    q.enqueue(Sources()._jigsaw_search, company_name)
    #Sources()._jigsaw_search(company_name)
    return {'started': True}

@app.route('/v1/companies/streaming', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_streaming_info():
    company = check_if_company_exists_in_db(request.args)
    if company != []: return company
    company= Companies()._get_info(request.args['company_name'])
    if str(company) == "not found": 
        return {company_name: "Not Found."}
    else: 
        q.enqueue(Parse()._add_company, company.ix[0].to_dict(), company_name)
        return company.ix[0].to_dict()

def remove_accents(input_str):
    try: input_str = unicode(input_str, 'utf8')
    except: "lol"
    nkfd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nkfd_form.encode('ASCII', 'ignore')
    return only_ascii

@app.route('/v1/companies', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_research():
    #TODO - check if api key is valid and increment request count
    #TODO - add name if name is present
    company_name = remove_accents(request.args['company_name'])
    #api_key = request.args['api_key']
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    qry = {'where':json.dumps({'company_name':company_name})}
    company = Parse().get('Company', qry).json()['results']
    name = ""
    print company
    if company:
        q.enqueue(Webhook()._update_company_info, company[0], api_key, name)
        return company[0]
    else:
        q.enqueue(Companies()._research, company_name, api_key, name)
        return {'Research has started.': True}

@app.route('/v1/companies/research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_research():
    if 'bulk' in request.args.keys():
        q.enqueue(Companies()._bulk, request.args['company_name'], request.args['api_key'])
    else:
        q.enqueue(Companies()._research, request.args['company_name'], request.args['api_key'])
      
    return {'Research has started.': True}

'''  ******************** Second Thing - EmailGuess **************************  '''

@app.route('/v1/mx_search', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def mx_search():
    name, domain = request.args['name'], request.args['domain']
    q.enqueue(Sources()._mx_server_check, name, domain)
    return {'started': True}

@app.route('/v1/email_pattern/research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def email_pattern_research():
    website = request.args['domain']
    domain = "{}.{}".format(tldextract.extract(website).domain,
                            tldextract.extract(website).tld)
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    name = request.args['name'] if "name" in request.args.keys() else ""
    q.enqueue(EmailGuess().search_sources, domain, name, api_key, timeout=6000)
    return {'email_research_started':True}

@app.route('/v1/email_pattern', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def email_research():
    #TODO - add name support
    website = request.args['domain']
    domain = "{}.{}".format(tldextract.extract(website).domain,
                            tldextract.extract(website).tld)
    name = request.args['name'] if "name" in request.args.keys() else ""
    pattern = Parse().get('EmailPattern', {'domain':domain}).json()
    try: pattern = pattern['results']
    except: print pattern
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    if pattern:
        pattern = {'domain':domain, 'company_email_pattern': pattern[0]['company_email_pattern']}
        q.enqueue(Webhook()._update_company_email_pattern, pattern)
        #Webhook()._post(api_key, pattern, 'email_pattern')
        return pattern
    else:
        q.enqueue(EmailGuess().search_sources, domain, name, api_key, timeout=6000)
        return {'started': True}

'''  ************************** Employee Stuff **************************  '''

@app.route('/v1/employees/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def employees_webhook():
    args = request.args
    company, prospect_list = args['company_name'], args['prospect_list']
    q.enqueue(MiningJob().employee_webhook, company, prospect_list)
    return {'started':True }

@app.route('/v1/company_list/employees',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_list_employees_webhook():
    args = request.args
    company_list_id, list_name = args['company_list'], args['list_name']
    title_qry, limit = args['title_qry'], request.args['limit']
    q.enqueue(MiningJob()._company_list_employees, 
              company_list_id, list_name, title_qry, limit)
    return {'started':True }

'''  ************************** Scoring **************************  '''

@app.route('/v1/score/email_pattern',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def score_email_pattern():
    domain = json.loads(request.args['domain'])['object']['domain']
    #api_key = json.loads(request.args['domain'])['object']['api_key']
    #domain = request.args['domain']
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    print domain
    q.enqueue(Score()._email_pattern, domain, api_key)
    return {'started': True}

@app.route('/v1/score/company_info',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def score_company_info():
    # Company Info objectId 
    print request.args
    domain = json.loads(request.args['company_name'])['object']['company_name']
    #api_key = json.loads(request.args['company_name'])['object']['api_key']
    #domain = "guidespark"
    #domain = request.args['company_name']
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    q.enqueue(CompanyScore()._company_info, domain, api_key)
    return {'started': True}

@app.route('/v1/test/score/email_pattern',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def test_score_email_pattern():
    domain = request.args['domain']
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    print domain
    q.enqueue(Score()._email_pattern, domain, api_key)
    return {'started': True}

@app.route('/v1/test/score/company_info',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def test_score_company_info():
    # Company Info objectId 
    print request.args
    #domain = json.loads(request.args['company_name'])['object']['company_name']
    #api_key = json.loads(request.args['company_name'])['object']['api_key']
    #domain = "guidespark"
    company_name = request.args['company_name']
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    q.enqueue(CompanyScore()._company_info, company_name, api_key)
    return {'started': True}

'''  ****************************************************

          PreComputed Reports - ClearSpark Effectivness
     
****************************************************  '''

'''  **************************

        Helper Functions
     
**************************  '''

def check_if_company_exists_in_db(args):
    parse, company_name = Parse(), args['company_name']
    qry = {'search_queries': company_name}
    company = Parse().get('Company', {'where': json.dumps(qry)}).json()['results']
    return company

def check_if_email_pattern_exists(args):
    parse, google = Parse(), Google()
    domain = tldextract.extract(args['domain'])
    domain = "{}.{}".format(domain.domain, domain.tld)

    qry = json.dumps({'domain': domain})
    qry = {'where':qry, 'include':'company_email_pattern'}
    pattern = parse.get('CompanyEmailPattern', qry).json()

@app.route('/v1/secondary_research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def secondary_research():
    name = "Coast Inc"
    domain = "onecoast.com"
    q.enqueue(Companies()._secondary_research, name, domain, timeout=600)
    return {'started':True}

@app.route('/hirefire/a6b3b40a4717a3c2e023751cb0f295a82529b2a5/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def get_job_count():
    #return {"job count": len(q.jobs)}
     return [{"name": "worker", "quantity" : len(q.jobs)}]

@app.route('/', methods=['GET'])
def test():
    return {"test": "lol"}
  
@app.route('/v1/replenish', methods=['GET'])
def replenish_research():
  company_name = "Electronic Arts"
  q.enqueue(Jigsaw()._replenish, company_name)
  return {"started": True}

@app.route('/v1/domain_research', methods=['GET'])
def domain_research():
    name = request.args["name"]
    domain = request.args["domain"]
    q.enqueue(Companies()._domain_research, domain, "", name)
    return {"test": "lol"}

@app.route('/v1/glassdoor', methods=['GET'])
def glassdoor():
    name = request.args["name"]
    q.enqueue(GlassDoor()._reviews, name)
    return {"test": "lol"}

if __name__ == "__main__":
    app.run(debug=True, port=4000)

'''
https://github.com/rsimba/heroku-xvfb-buildpack.git
https://github.com/tstachl/heroku-buildpack-selenium.git
https://github.com/srbartlett/heroku-buildpack-phantomjs-2.0.git
https://github.com/leesei/heroku-buildpack-casperjs.git
'''
# lol
