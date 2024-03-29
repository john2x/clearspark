from flask.ext.api import FlaskAPI
from li import Linkedin
from integrations import Integrations
import json
from social import *
from zoominfo import Zoominfo
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

@app.route('/v1/bulk/email_pattern/research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _email_pattern_research():
    companies = Parse().get("Company", {"order":"-createdAd", "limit":1000}) 
    for company in companies.json()["results"]:
        if "domain" in company.keys():
            domain = company["domain"]
            api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
            #name = request.args['name'] if "name" in request.args.keys() else ""
            name = ""
            q.enqueue(EmailGuess().search_sources, domain, name, api_key, timeout=6000)
    return {"research":"started"}

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
    #print "ARGS", requests.args
    print "DATA", request.data
    name, domain = request.data["company_name"], request.data["domain"]
    q.enqueue(Companies()._secondary_research, name, domain, timeout=600)
    return {'started':True}

@app.route('/v1/test/daily_research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def test_daily_research():
    name, domain = request.args["company_name"], request.args["domain"]
    print name, domain
    q.enqueue(Companies()._daily_secondary_research, name, domain, timeout=6000)
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

@app.route('/v1/research_signal_report', methods=['GET'])
def research_report():
    report = request.args["report"]
    q.enqueue(Companies()._research_report, report)
    return {"test": "lol"}

@app.route('/v1/score_signal_report', methods=['GET'])
def score_report():
    report = request.args["report"]
    q.enqueue(Companies()._score_report, report)
    return {"test": "lol"}

@app.route('/v1/glassdoor', methods=['GET'])
def glassdoor():
    name = request.args["name"]
    q.enqueue(GlassDoor()._reviews, name)
    return {"test": "lol"}

@app.route('/v1/company_webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_webhook():
    domain, company_name = request.data["domain"], request.data["company_name"]
    q.enqueue(Companies()._secondary_research, company_name, domain)
    return {'started':True}

@app.route('/v1/company_signal_webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_signal_webhook():
    q.enqueue(Companies()._research, request.data["company_name"])
    return {'started':True}

@app.route('/v1/company_prospect_webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_prospect_webhook():
    q.enqueue(Companies()._research, request.data["company_name"])
    return {'started':True}

@app.route('/v1/daily_news', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _daily_news():
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    q.enqueue(Companies()._daily_secondary_research, 
              request.args["company_name"],
              request.args["domain"], api_key)
    return {'started':True}

@app.route('/v1/daily_news_source', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _daily_news_source():
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    name, domain = request.args["company_name"], request.args["domain"]

    x = 6000
    if "blog" == request.args["source"]:
      j0 = q.enqueue(Companies()._company_blog, domain, api_key, name, timeout=x)
    elif "glassdoor" == request.args["source"]:
      j2 = q.enqueue(GlassDoor()._reviews, domain, api_key, name, timeout=x)
    elif "press" == request.args["source"]:
      j3 = q.enqueue(Companies()._press_releases, domain, api_key, name, timeout=x)
    elif "news" == request.args["source"]:
      j4 = q.enqueue(Companies()._news, domain, api_key, name, timeout=x)
    elif "hiring" == request.args["source"]:
      j5 = q.enqueue(Companies()._hiring, domain, api_key, name, timeout=x)
    elif "twitter" == request.args["source"]:
      j6 = q.enqueue(Twitter()._daily_news, domain, api_key, name, timeout=x)
    elif "facebook" == request.args["source"]:
      j7 = q.enqueue(Facebook()._daily_news, domain, api_key, name, timeout=x)
    elif "linkedin" == request.args["source"]:
      j8 = q.enqueue(Linkedin()._daily_news, domain, api_key, name, timeout=x)

    return {'started':True}

@app.route('/v1/company_name_source', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_name_source():
    api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
    name = request.args["company_name"]

    if "facebook" in request.args["source"]:
      j9 = q.enqueue(Facebook()._company_profile, name, api_key,timeout=6000)
    elif "twitter" in request.args["source"]:
      j10 = q.enqueue(Twitter()._company_profile, name, api_key,timeout=6000)
    elif "indeed" in request.args["source"]:
      j11 = q.enqueue(Indeed()._company_profile, name, api_key,timeout=6000)
    elif "businessweek" in request.args["source"]:
      j0 =q.enqueue(BusinessWeek()._company_profile, name, api_key,timeout=6000)
    elif "zoominfo" in request.args["source"]:
      j1 = q.enqueue(Zoominfo()._company_profile, name, api_key,timeout=6000)
    elif "linkedin" in request.args["source"]:
      j2 = q.enqueue(Linkedin()._company_profile, name, api_key,timeout=6000)
    elif "yellowpages" in request.args["source"]:
      j3 = q.enqueue(YellowPages()._company_profile, name, api_key,timeout=6000)
    elif "yelp" in request.args["source"]:
      j4= q.enqueue(Yelp()._company_profile, name, api_key,timeout=6000)
    elif "forbes" in request.args["source"]:
      j5 = q.enqueue(Forbes()._company_profile, name, api_key,timeout=6000)
    elif "glassdoor" in request.args["source"]:
      j6 = q.enqueue(GlassDoor()._company_profile, name, api_key,timeout=6000)
    elif "hoovers" in request.args["source"]:
      j7 = q.enqueue(Hoovers()._company_profile, name, api_key,timeout=6000)
    elif "crunchbase" in request.args["source"]:
      j8 = q.enqueue(Crunchbase()._company_profile, name, api_key,timeout=6000)
    return {'started':True}

@app.route("/v1/company_bulk_upload", methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _company_bulk_upload():
    #TODO - three fields, company_name, website, domain
    # - really only need 1 
    data = request.args["data"]
    user = request.args["user"]
    q.enqueue(Companies()._bulk_upload, data, user)
    return {"started":True}

@app.route("/v1/integration", methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def _add_integration():
    #TODO - three fields, company_name, website, domain
    # - really only need 1 
    token = request.args["token"]
    user = request.args["user"]
    user_company = request.args["user_company"]

    print request.args
    print token, user, user_company, request.args["source"]
    if "google" in request.args["source"]:
      q.enqueue(Integrations()._google_contact_import, token, user, user_company)
    elif "salesforce" in request.args["source"]:
      print "SALESFORCE"
      instance_url = request.args["instance_url"]
      q.enqueue(Integrations()._salesforce_import, token, instance_url, 
                user, user_company)
    return {"started":True}

if __name__ == "__main__":
    app.run(debug=True, port=4000)

'''
https://github.com/rsimba/heroku-xvfb-buildpack.git
https://github.com/tstachl/heroku-buildpack-selenium.git
https://github.com/srbartlett/heroku-buildpack-phantomjs-2.0.git
https://github.com/leesei/heroku-buildpack-casperjs.git
'''
# lol
