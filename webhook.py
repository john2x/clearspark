from parse import Prospecter as Parse
from parse import Parse as ClearSpark
import requests
import json
import unicodedata
import pusher
#from email_guess import EmailGuess
import arrow
import pandas as pd
import time

from rq import Queue
from worker import conn
q = Queue(connection=conn)

_pusher = pusher.Pusher(
  app_id='105534',
  key='950f66be1f764448120e',
  secret='5c7a91f7e0da71c57dbf'
)

class Webhook:
    def _post(self, api_key, data, hook_type):
        print "WEBHOOK WEBHOOK WEBHOOK"
        qry = {'where':json.dumps({'api_key':api_key, 'hook_type' :hook_type})}
        webhooks = Parse().get('Webhook', qry).json()['results']
        for hook in webhooks:
            print hook['url']
            print data
            headers = {'content-type': 'application/json'}
            r = requests.post(hook['url'], data=json.dumps(data), headers=headers)
            print 'webhook', r, data, r.text

    def remove_accents(self, input_str):
        try: input_str = unicode(input_str, 'utf8')
        except: "lol"
        nkfd_form = unicodedata.normalize('NFKD', input_str)
        only_ascii = nkfd_form.encode('ASCII', 'ignore')
        return only_ascii

    def _unparsify_data(self, data):
        if 'className' in data.keys(): del data['className']
        if '__type' in data.keys(): del data['__type']
        if 'objectId' in data.keys(): del data['objectId']
        if 'createdAt' in data.keys(): del data['createdAt']
        if 'updatedAt' in data.keys(): del data['updatedAt']
        
        return data

    def _update_company_info(self, data, api_key="", name=""):
        print "DATA"
        print data 
        company_name = self.remove_accents(data['company_name'])
        qry = {'where':json.dumps({'company_name':data['company_name']})}
        qry_1 ={'where':json.dumps({'company_name': company_name})}
        qry = {"where":json.dumps({"$or":[{"company_name":data["company_name"], "company_name": company_name}]})}
        company = Parse().get('Company', qry).json()
        while "results" not in company.keys():
            time.sleep(0.5)
            company = Parse().get('Company', qry).json()

        companies = company['results']
        data = self._unparsify_data(data)

        if companies == []:
            company = Parse().create('Company', data).json()
            while "objectId" not in company.keys():
                time.sleep(0.5)
                company = Parse().create('Company', data).json()
                print "retrying", company
            print company
            companies = [Parse()._pointer('Company',company['objectId'])]

        print data["company_name"]
        company_name = data["company_name"].replace(' ','-')
        #_pusher['customero'].trigger(company_name, {'company': data})

        print "__STARTED", len(companies)
        for company in companies:
            print "UPDATING COMPANY"
            #TODO batch_df update
            print Parse().update('Company/'+company['objectId'], data).json()
            _company = Parse()._pointer('Company', company['objectId'])
            classes = ['Prospect','CompanyProspect','PeopleSignal','CompanySignal']
            objects = []
            for _class in classes:
                df = pd.DataFrame()
                objects = Parse().get(_class, qry).json()['results']
                data = {'company':_company, 
                        'company_research': arrow.utcnow().timestamp}
                df["objectId"] = [i["objectId"] for i in objects]
                Parse()._batch_df_update(_class, df, data)

            #TODO - batch update
            for obj in objects:
                print "UPDATED", _class, obj
                _id = obj['objectId']
                print Parse().update(_class+"/"+_id, data).json()
                #TODO - add name email guess - what is this code below
                name = ""
                if _class == 'Prospect':
                    print company
                    domain = company["domain"]
                    #q.enqueue(EmailGuess().search_sources, domain, "", api_key)

        return "updated"

        # TODO BATCHIFY
        print "CREATING COMPANY"
        company = Parse().create('Company', data).json()
        _company = Parse()._pointer('Company', company['objectId'])
        classes = ['Prospect','CompanyProspect','PeopleSignal','CompanySignal']
        for _class in classes:
            objects = Parse().get(_class, qry).json()['results']
            for obj in objects:
                print "UPDATED", _class, obj
                _id = obj['objectId']
                print Parse().update(_class+"/"+_id, {'company':_company}).json()
                p['customero'].trigger(data["company_name"], {'company': data})

    def _update_company_email_pattern(self, data):
        if not data: return 0
        qry = {'where':json.dumps({'domain': data['domain']})}
        companies = Parse().get('Company', qry).json()
        while "error" in companies.keys():
            time.sleep(3)
            companies = Parse().get('Company', qry).json()
        companies = companies['results']
        pattern = {'email_pattern': data['company_email_pattern']}
        if data['company_email_pattern'] == []: 
            pattern['email_guess'] = []
        #_pusher['customero'].trigger(data["domain"], pattern)
        for company in companies:
            #print data
            data = {'email_pattern':data['company_email_pattern'], 
                    'email_pattern_research': arrow.utcnow().timestamp}
            r = Parse().update('Company/'+company['objectId'], data)
            # pusher -->
            print r.json()
            try:
              ''' print data["domain"] '''
            except:
              ''' print "wtf error ", data '''
            #ClearSpark().get('Company',{'where':json.dumps({})})
