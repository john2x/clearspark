from companies import Companies
from parse import Prospecter, Parse
import json
import arrow
from queue import RQueue

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class MiningJob:
    def employee_webhook(self, company_name, company_list,qry="",limit=5, list_id="", _report=""):
        _user, _company = company_list['user'], company_list['company']
        employees = Companies()._employees(company_name, qry)
        company = Companies()._get_info(company_name)
        _company_list = company_list['objectId']
        for index, row in employees.iterrows():
            data = row.to_dict()
            company['user'], company['company'] = _user, _company
            prospect = company
            prospect['name'], prospect['pos'] = row['name'], row['title']
            prospect['city'] = row['locale']
            prospect['linkedin_url'] = row['linkedin_url']
            prospect['lists'] = [Parse()._pointer('ProspectList', list_id)]
            if type(company['industry']) is list: 
                company['industry'] = company['industry'][0]
            prospect['company_profile'] = company_list['profile']
            r = Prospecter().create('Prospect', company)
            print "prospect_create_result", r.json()

        if RQueue()._has_completed("{0}_{1}".format(_company_list, list_id)):
            print "employee_webhook_has_completed"
            print Parse("SignalReport/"+_report, {'done':arrow.now()}).json()

    def company_list_employee_webhook(self, company_list,qry="",limit=0,prospect_list=""):
        qry = {"lists": Parse()._pointer("CompanyProspectList",company_list)}
        rr = Parse().get('CompanyProspect', {'where':json.dumps(qry)})
        for company in rr.json()['results']:
           self.employee_webhook(company['name'], 
                                 company['user']['objectId'], 
                                 company['company']['objectId'], 
                                 qry, limit, prospect_list) 

    def _add_reports(self, list_name, companies, company_list, _profile):
        company_list_id = company_list['objectId']
        _user, _company = companies[0]['user'], companies[0]['company']
        data = {'name': list_name, 'user':_user, 'company':_company}
        _company_list = Parse()._pointer('CompanyProspectList', company_list_id)
        data['parent_list'], data['list_type'] = _company_list, 'mining_job'
        _report = {'report_type':'company_employee_mining_job', 'profile': _profile}
        signal_report = Parse().create('SignalReport', _report).json()
        _report = Parse()._pointer('SignalReport', signal_report['objectId'])
        _list_id = Prospecter().create('ProspectList', data).json()['objectId']
        _prospect_list = Parse()._pointer('ProspectList', _list_id)
        _report = {'reports':{'__op':'AddUnique', 'objects':[_report]}}
        _list = {'prospect_lists':{'__op':'AddUnique', 'objects':[_prospect_list]}}
        r = Prospecter().update('CompanyProspectList/'+company_list_id, _list)
        rr = Prospecter().update('ProspectProfile/'+_profile['objectId'], _report)
        print r.json(), rr.json()

        # TODO - create signal_report
        # TODO - update company_prospectlist with mining job
        # TODO - update prospectlist with parent company_prospectlist
        # TODO - update companyprospectlist with child prospectlist
        # TODO - update prospectprofile with signal_report
        return (signal_report['objectId'], _list_id)
    
    def _company_list_employees(self, company_list_id, list_name, title, limit):
        company_list = Prospecter().get('CompanyProspectList/'+company_list_id).json()
        print company_list
        _profile= company_list['profile']
        qry = {"lists":Prospecter()._pointer("CompanyProspectList", company_list_id)}
        qry = {'where':json.dumps(qry),'order':'-createdAt'}
        companies = Prospecter().get('CompanyProspect', qry).json()['results']
        _report, _list = self._add_reports(list_name,companies,company_list,_profile)

        queue_name = "{0}_{1}".format(company_list_id, _list)
        for company in companies:
            job = q.enqueue(self.employee_webhook, company['name'], 
                            company_list, title, limit, _list, _report)
            job.meta[queue_name] = True; job.save()
        return {'started':True }

    def clean_non_ascii(text):
        return "".join(" " if ord(i)>128 else i for i in text)
