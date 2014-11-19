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
from sources import prnewswire_google_search
from sources import businesswire_google_search
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
    pw = google.search('"{0}" site:prnewswire.com'.format(domain))
    bw = google.search('"{0}" site:businesswire.com'.format(domain))

    job_queue_lol = domain+str(arrow.now().timestamp)
    for link in pw.link: 
        print "STARTED", pw.shape
        job = q.enqueue(prnewswire_google_search, domain, link,
                        job_queue_lol, timeout=3600)
        job.meta['profile_id1'] = job_queue_lol
        job.save()
    for link in bw.link: 
        print "BW STARTED", bw.shape
        job = q.enqueue(businesswire_google_search, domain, link,
                        job_queue_lol, timeout=3600)
        job.meta['profile_id1'] = job_queue_lol
        job.save()

    if pattern == []:
      return {'queued': True}
    else:
      return pattern

@app.route('/v1/companies/streaming/domain', methods=['GET','OPTIONS','POST'])
@crossdomain(origin='*')
def search():
    # search google for business wire
    ''' '''

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
