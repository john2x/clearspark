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
    domain = tldextract.extract(request.args['domain'])
    domain = "{}.{}".format(domain.domain, domain.tld)

    qry = json.dumps({'domain': domain})
    qry = {'where':qry, 'include':'company_email_pattern'}
    pattern = parse.get('CompanyEmailPattern', qry).json()
    print pattern
    q.enqueue(EmailGuess().start_search, domain)

    if pattern['results'] == []: return {'queued': True}
    else: return pattern

@app.route('/v1/companies/streaming/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def search():
    parse, google = Parse(), Google()
    domain = tldextract.extract(request.args['domain'])
    domain = "{}.{}".format(domain.domain, domain.tld)

    qry = json.dumps({'domain': domain})
    qry = {'where':qry, 'include':'company_email_pattern'}
    pattern = parse.get('CompanyEmailPattern', qry).json()
    print pattern
    EmailGuess().streaming_search(domain)

    if pattern['results'] == []: return {'queued': True}
    else: return pattern

#TODO - add webhook support

@app.route('/', methods=['GET'])
def test():
    return {"test": "lol"}

@app.route('/hirefire/a6b3b40a4717a3c2e023751cb0f295a82529b2a5/info', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def get_job_count():
    #return {"job count": len(q.jobs)}
     return [{"name": "worker", "quantity" : len(q.jobs)}]

if __name__ == "__main__":
    app.run(debug=True)
