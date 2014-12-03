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
from sources import PRNewsWire
from sources import BusinessWire
from email_patterns import EmailGuess
from companies import Companies
import pandas as pd
import json
import email_patterns

from rq import Queue
from worker import conn
q = Queue(connection=conn)

app = FlaskAPI(__name__)

@app.route('/v1/companies/streaming/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_streaming_info():
    company = check_if_company_exists_in_db(request.args)
    if company != []: return company
    company= Companies()._get_info(company_name)
    if str(company) == "not found": return {company_name: "Not Found."}
    else: 
        q.enqueue(Parse()._add_company, company.ix[0].to_dict(), company_name)
        return company.ix[0].to_dict()

@app.route('/v1/companies/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def company_info():
    print "started"
    company = check_if_company_exists_in_db(request.args)
    q.enqueue(Companies()._async_get_info, company_name)
    if company != []: return company
    else: return {'queued': 'The search query has been queued. Please check back soon.'}

@app.route('/v1/app/companies/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def app_company_info():
    company = check_if_company_exists_in_db(request.args)
    q.enqueue(Companies()._async_get_info, company_name, request.args['objectId'])
    if company != []: return company
    else: return {'queued': 'The search query has been queued. Please check back soon.'}
    
@app.route('/v1/companies/webhook', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def app_company_info():
    company = check_if_company_exists_in_db(request.args)
    q.enqueue(Companies()._get_info_webhook, company_name, request.args['objectId'])
    if company != []: return company
    else: return {'queued': 'The search query has been queued. Please check back soon.'}


'''  **************************

     Second Thing - ClearSpark 
     
**************************  '''

@app.route('/v1/companies/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def find_email_address():
    pattern = check_if_email_pattern_exists(request.args)
    q.enqueue(EmailGuess().start_search, domain)

    if pattern['results'] == []: return {'queued': True}
    elif pattern['results'][0]['company_email_pattern'] == []: 
        return {'Error': "Domain email pattern could not be found. Retrying. Please retry again soon."}
    else: return pattern['results'][0]

@app.route('/v1/companies/streaming/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def search():
    pattern = check_if_email_pattern_exists(request.args)
    EmailGuess().streaming_search(domain)
    if pattern['results'] == []: return {'queued': True}
    else: return pattern

@app.route('/hirefire/a6b3b40a4717a3c2e023751cb0f295a82529b2a5/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def get_job_count():
    #return {"job count": len(q.jobs)}
     return [{"name": "worker", "quantity" : len(q.jobs)}]

if __name__ == "__main__":
    app.run(debug=True)

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
