from parse import Prospecter as Parse
from parse import Parse as ClearSpark
import requests
import json
import unicodedata
import pusher
from email_guess import EmailGuess
import arrow

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
        print data 
        company_name = self.remove_accents(data['company_name'])
        qry = {'where':json.dumps({'company_name':data['company_name']})}
        qry_1 ={'where':json.dumps({'company_name': company_name})}
        company = Parse().get('Company', qry_1).json()['results']
        companies = company + Parse().get('Company', qry).json()['results']

        data = self._unparsify_data(data)
        if companies == []:
            company = Parse().create('Company', data).json()
            print company
            companies = [Parse()._pointer('Company',company['objectId'])]

        _pusher = pusher.Pusher(
          app_id='105534',
          key='950f66be1f764448120e',
          secret='5c7a91f7e0da71c57dbf'
        )

        print data["company_name"]
        company_name = data["company_name"].replace(' ','-')
        _pusher['customero'].trigger(company_name, {'company': data})

        print "__STARTED", len(companies)
        for company in companies:
            print "UPDATING COMPANY"
            print Parse().update('Company/'+company['objectId'], data).json()
            _company = Parse()._pointer('Company', company['objectId'])
            classes = ['Prospect','CompanyProspect','PeopleSignal','CompanySignal']
            for _class in classes:
                objects = Parse().get(_class, qry).json()['results']
                print "OBJECTS FOUND WITH COMPANY", objects
                for obj in objects:
                    print "UPDATED", _class, obj
                    _id = obj['objectId']
                    data = {'company':_company, 'company_research': arrow().utcnow()}
                    print Parse().update(_class+"/"+_id, data).json()
                #TODO - add name email guess - what is this code below
                name = ""
                if _class == 'Prospect':
                    domain = company["domain"]
                    q.enqueue(EmailGuess().search_sources, domain, "", api_key)

        return "updated"

        print "CREATING COMPANY"
        company = Parse().create('Company', data).json()
        _company = Parse()._pointer('Company', company['objectId'])
        classes = ['Prospect','CompanyProspect','PeopleSignal','CompanySignal']
        p = pusher.Pusher(
          app_id='70217',
          key='1a68a96c8fde938fa75a',
          secret='e60c9259d0618b479ec2'
        )
        for _class in classes:
            objects = Parse().get(_class, qry).json()['results']
            for obj in objects:
                print "UPDATED", _class, obj
                _id = obj['objectId']
                print Parse().update(_class+"/"+_id, {'company':_company}).json()
                p['customero'].trigger(data["company_name"], {'company': data})

    def _update_company_email_pattern(self, data):
        print data
        if not data: return 0
        qry = {'where':json.dumps({'domain': data['domain']})}
        companies = Parse().get('Company', qry).json()['results']
        pattern = {'email_pattern': data['company_email_pattern']}
        if data['company_email_pattern'] == []: 
            pattern['email_guess'] = EmailGuess()._random()
        _pusher['customero'].trigger(data["domain"], pattern)
        for company in companies:
            data = {'email_pattern':data['company_email_pattern'], 
                    'email_pattern_research': arrow().utcnow()}
            r = Parse().update('Company/'+company['objectId'], data)
            # pusher -->
            print r.json()

            print data["domain"]
            #ClearSpark().get('Company',{'where':json.dumps({})})
