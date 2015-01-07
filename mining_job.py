from companies import Companies
from parse import Prospecter, Parse
import json

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class MiningJob:
    def employee_webhook(self, company_name, user_id, company_id, qry="",limit=5, list_id=""):
        print "the_employee_webhook"
        _user = Parse()._pointer('_User', user_id)
        _company = Parse()._pointer('Company', company_id)
        employees = Companies()._employees(company_name, qry)
        print "the_employees_employee", employees.to_dict('records')
        company = Companies()._get_info(company_name)
        _user = Parse()._pointer('User', user_id)
        for index, row in employees.iterrows():
            data = row.to_dict()
            print "row_to_persist", data
            print "company_to_persist", company
            company['user'], company['company'] = _user, _company
            prospect = company
            prospect['name'], prospect['pos'] = row['name'], row['title']
            prospect['city'], prospect['linkedin_url'] = row['locale'], row['linkedin_url']
            prospect['lists'] = [Parse()._pointer('ProspectList', list_id)]
            if type(company['industry']) is list: 
                company['industry'] = company['industry'][0]
            # TODO - prospect profile add prospect_profiles
            r = Prospecter().create('Prospect', company)
            print "prospect_create_result", r.json()

    def company_list_employee_webhook(self, company_list, qry="", limit=0,prospect_list=""):
        qry = {"lists":Parse()._pointer("CompanyProspectList",company_list)}
        rr = Parse().get('CompanyProspect', {'where':json.dumps(qry)})
        for company in rr.json()['results']:
           self.employee_webhook(company['name'], 
                                 company['user']['objectId'], 
                                 company['company']['objectId'], 
                                 qry, limit, prospect_list) 
    
    def _company_list_employee_webhook(self, company_list_id, list_name,title,limit):
        qry = {"lists":Prospecter()._pointer("CompanyProspectList", company_list_id)}
        qry = {'where':json.dumps(qry),'order':'-createdAt'}
        companies = Prospecter().get('CompanyProspect', qry).json()['results']
        _user, _company = companies[0]['user'], companies[0]['company']
        data = {'name': list_name, 'user':_user, 'company':_company}
        _list_id = Prospecter().create('ProspectList', data).json()['objectId']

        for company in companies:
            print "789789", company
            q.enqueue(self.employee_webhook, company['name'], 
                           company['user']['objectId'], 
                           company['company']['objectId'], 
                           title, limit, _list_id)
        return {'started':True }

    def clean_non_ascii(text):
        return "".join(" " if ord(i)>128 else i for i in text)
