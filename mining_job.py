from companies import Companies
from parse import Parse

class MiningJob:
    def employee_webhook(self, company_name, user_id, company_id, qry="",limit=5, list_id=""):
        _user = Parse()._pointer('_User', user_id)
        _company = Parse()._pointer('Company', company_id)
        employees = Companies()._employees(company_name, qry)
        company = Companies()._get_info(company_name)
        _user = Parse()._pointer('User', user_id)
        # partial token above 85
        for index, row in employees.iterrows():
            data = row.to_dict()
            print data
            company['user'], company['company'] = _user, _company
            prospect = company
            prospect['name'] = row['name']
            prospect['pos'] = row['title']
            prospect['city'] = row['locale']
            prospect['lists'] = [Parse()._pointer('ProspectList', list_id)]
            r = Parse().create('Prospect', company, True)
            print r.json()
            #if index > limit: break

    def company_list_employee_webhook(self, company_list, qry="", limit=0,prospect_list=""):
        ''' '''
        qry = {"lists":Parse()._pointer("CompanyProspectList",company_list)}
        rr = Parse().get('CompanyProspect', {'where':json.dumps(qry)})
        for company in rr.json()['results']:
           self.employee_webhook(company['name'], 
                                 company['user']['objectId'], 
                                 company['company']['objectId'], 
                                 qry, limit, prospect_list) 
    def clean_non_ascii(text):
        return "".join(" " if ord(i)>128 else i for i in text)
