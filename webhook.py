from parse import Prospecter as Parse
import requests
import json

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

    def _update_company_info(self, data):
        company_name = self.remove_accents(data['company_name'])
        qry = {'where':json.dumps({'company_name':data['company_name']})}
        qry_1 ={'where':json.dumps({'company_name': company_name})}
        company = Parse().get('Company', qry_1).json()['results']
        companies = company + Parse().get('Company', qry).json()['results']

        if 'className' in data.keys(): del data['className']
        if '__type' in data.keys(): del data['__type']
        if 'objectId' in data.keys(): del data['objectId']
        if 'createdAt' in data.keys(): del data['createdAt']
        if 'updatedAt' in data.keys(): del data['updatedAt']

        for company in companies:
            print "UPDATING COMPANY"
            print Parse().update('Company/'+company['objectId'], data).json()

            _company = Parse()._pointer('Company', company['objectId'])
            classes = ['Prospect','CompanyProspect','PeopleSignal','CompanySignal']
            for _class in classes:
                objects = Parse().get(_class, qry).json()['results']
                for obj in objects:
                    print "UPDATED", _class, obj
                    _id = obj['objectId']
                    print Parse().update(_class+"/"+_id, {'company':_company}).json()
        return "updated"

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

    def _update_company_email_pattern(self, data):
        print data
        if not data: return 0
        qry = {'where':json.dumps({'domain': data['domain']})}
        companies = Parse().get('Company', qry).json()['results']
        for company in companies:
            r = Parse().update('Company/'+company['objectId'], {'email_pattern':data['company_email_pattern']})
            print r.json()
        
