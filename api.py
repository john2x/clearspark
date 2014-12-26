from flask.ext.api import FlaskAPI
import pusher
from flask import request
import requests
import json
import tldextract
import arrow
from parse import Parse
from crossdomain import crossdomain
from google import Google
import toofr
import requests
from parse import Parse
from google import Google
from bs4 import BeautifulSoup
from nameparser import HumanName
from press_sources import PRNewsWire
from press_sources import BusinessWire
from email_guess import EmailGuess
from companies import Companies
import pandas as pd
import json
import email_guess
from sources import Sources
from mining_job import MiningJob
from score import Score

from rq import Queue
from worker import conn
q = Queue(connection=conn)

app = FlaskAPI(__name__)

@app.route('/v1/companies/streaming/info', methods=['GET','OPTIONS','POST'])
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

@app.route('/v1/companies/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_info():
    q.enqueue(Companies()._async_get_info, request.args['company_name'])
    company = check_if_company_exists_in_db(request.args)
    return company if company else {'': 'Your query has been queued.'}

@app.route('/v1/app/companies/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def app_company_info():
    q.enqueue(Companies()._async_get_info, company_name, request.args['objectId'])
    company = check_if_company_exists_in_db(request.args)
    return company if company else {'': 'Your query has been queued.'}
    
@app.route('/v1/companies/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def app_company_info_webhook():
    company = check_if_company_exists_in_db(request.args)
    q.enqueue(Companies()._get_info_webhook, request.args['company_name'], 
                                             request.args['objectId'],
                                             timeout=3600)
    if company: 
        r = Parse().update('CompanyProspect/'+request.args['objectId'], company[0], True)
        rr = Parse().update('Prospect/'+request.args['objectId'], company[0], True)
        print "RESULTS - ", r.json(), rr.json()
    return company[0] if company else {'': 'Your query has been queued.'}

@app.route('/v1/companies/research', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_research():
    q.enqueue(Companies()._research, request.args['company_name'])
    return {'Research has started.' : True}

'''  **************************

     Second Thing - EmailGuess
     
**************************  '''

@app.route('/v1/companies/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def find_email_address():
    q.enqueue(EmailGuess().start_search, domain)
    pattern = check_if_email_pattern_exists(request.args)['results']

    if pattern == []: 
        return {'queued': True}
    elif pattern[0]['company_email_pattern']:
        return {'Error': "Domain email could not be found. Retrying."}
    else: 
        return pattern[0]

@app.route('/v1/companies/streaming/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def search():
    pattern = check_if_email_pattern_exists(request.args)
    EmailGuess().streaming_search(domain)
    return {'queued':True} if pattern['results'] else pattern

@app.route('/v1/emails/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def find_email_address_webhook():
    domain = request.args['domain']
    objectId = request.args['objectId']
    q.enqueue(EmailGuess().search_webhook, domain, objectId)
    #pattern = check_if_email_pattern_exists(request.args)
    return {'started': True}

@app.route('/v1/new_emails/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def find_new_email_address_webhook():
    website = request.args['domain']
    domain = "{}.{}".format(tldextract.extract(website).domain,
                            tldextract.extract(website).tld)
    objectId = request.args['objectId'] if "objectId" in request.args.keys() else ""
    name = request.args['name'] if "name" in request.args.keys() else ""
    research = q.enqueue(EmailGuess().search_sources, domain, name,
                         timeout=6000)
    # what about sub jobs for this?
    #pattern = check_if_email_pattern_exists(request.args)
    return {'started': True}

'''       Employee Stuff        '''

@app.route('/v1/employees/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def employees_webhook():
    ''' Employees '''
    args = request.args
    q.enqueue(MiningJob().employee_webhook, args['company_name'], args['prospect_list'])
    return {'started':True }

@app.route('/v1/company_list/employees',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_list_employees_webhook():
    ''' Employees - (Add Scoring) '''
    qry = {"lists":Parse()._pointer("CompanyProspectList",request.args['company_list'])}
    rr = Parse().get('CompanyProspect', {'where':json.dumps(qry),'order':'-createdAt'}, True)
    _user = rr.json()['results'][0]['user']
    _company = rr.json()['results'][0]['company']
    data = {'name':request.args['prospect_list'], 
            'user':_user, 'company':_company}
    _list_id =Parse().create('ProspectList',data, True).json()['objectId']
    for company in rr.json()['results']:
        print company
        q.enqueue(MiningJob().employee_webhook, company['name'], 
                             company['user']['objectId'], 
                             company['company']['objectId'], 
                             request.args['qry'], 
                             request.args['limit'], 
                             _list_id) 
    return {'started':True }

'''  **************************

          Scoring
     
**************************  '''

@app.route('/v1/score/email_pattern',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def score_email_pattern():
    domain = request.args['domain']
    q.enqueue(Score()._email_pattern, domain)
    return {'started': True}

@app.route('/v1/score/company_info',methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def score_company_info():
    # Company Info objectId 
    domain = request.args['domain']
    q.enqueue(Score()._company_info, domain)
    return {'started': True}

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

@app.route('/hirefire/a6b3b40a4717a3c2e023751cb0f295a82529b2a5/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def get_job_count():
    #return {"job count": len(q.jobs)}
     return [{"name": "worker", "quantity" : len(q.jobs)}]

@app.route('/', methods=['GET'])
def test():
    return {"test": "lol"}

if __name__ == "__main__":
    app.run(debug=True, port=4000)
