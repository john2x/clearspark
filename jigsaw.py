from splinter import Browser
import time
import uuid
from employee_search import *
#from sources import Sources

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class Jigsaw:
    def _login(self):
        browser = Browser('phantomjs')
        browser.visit('https://connect.data.com/login')
        browser.find_by_css('#j_username').first.fill('robin@customerohq.com')
        browser.find_by_css('#j_password').first.fill('951562nileppeZ')
        browser.find_by_css('#login_btn').first.click()
        time.sleep(1)
        return browser

    def _company_panel(self, browser, company_name):
        browser.visit("http://connect.data.com/search#p=advancedsearch;;t=companies;;ss=tabchanged")
        browser.find_by_css("#findCompaniesTab").first.click()
        browser.find_by_name("companies")[1].fill(company_name)
        time.sleep(1)
        browser.find_by_css('.search-button')[-1].click()
        browser.find_by_css('.companyName')
        return browser

    def _search(self, company_name):
        print "jigsaw init", company_name
        browser = self._login()
        browser = self._company_panel(browser, company_name)
        print "finished"
        if len(browser.find_by_css('.companyName')):
            print "started jigsaw search"
            time.sleep(1)
            browser.find_by_css('.companyName').first.click()
            time.sleep(1)
            browser.find_by_css('.company-counts > a').first.click()
            time.sleep(1)
            browser.find_by_name('directDial').first.click()
            time.sleep(2)
            browser.find_by_css('.td-name > a').first.click()
            time.sleep(1)
            # TODO- BE CAREFUL Costs to click on this
            # TODO - Develop recharge strategy
            try:
                browser.find_by_css('#getDetailsLink').first.click()
                self._replenish(company_name)
            except:
                ""
            name = browser.find_by_css('.businesscard-contactinfo-name').first
            email = browser.find_by_css('.businesscard-contactinfo-email').first
            name, email = name.text, email.text 
            return name, email
  

    def _replenish(self, company_name):
        ''' Run Title Mining Job and Upload to Jigsaw'''
        #TODO - Turn domain into company_name
        queue_name = "{0}_{1}".format(company_name, uuid.uuid1())
        '''
        job_1 = q.enqueue(GoogleSearch()._employees, company_name)
        job_2 = q.enqueue(LinkedinTitleDir()._search, company_name)
        '''
        job_1 = q.enqueue(GoogleSearch().test, company_name)
        job_2 = q.enqueue(LinkedinTitleDir().test, company_name)
        jobs = [job_1, job_2]
        for job in jobs:
            RQueue()._meta(job, "queue_name", queue_name)
            RQueue()._meta(job, queue_name)
            RQueue()._meta(job, "company_name", company_name)

    def _upload_csv(self, company_name):
        print "UPLOAD || UPLOAD || UPLOAD"
        print "UPLOAD || UPLOAD || UPLOAD"
        print "UPLOAD || UPLOAD || UPLOAD"
        print "UPLOAD || UPLOAD || UPLOAD"
        print "UPLOAD || UPLOAD || UPLOAD"
        data = self._company_profile(company_name)
        self._employee_df(data)
        browser = self._login()
        url = "https://connect.data.com/contact/"
        url = url + "options?bulkUpload=true&bulkOnly=true"
        browser.visit(url)
        browser.attach_file("file","/tmp/emp.csv")
        return False

    def _company_profile(company_name):
        companies = Google().search("site:data.com/company/view {0}".format(company_name))
        bs = BeautifulSoup(Google().cache(companies.ix[0].link))
        data = {}
        for row in bs.find("table").find_all("tr"):
            data[row.find_all("td")[0].text] = row.find_all("td")[-1].text.strip()
        
        cols = ["Address Line1", "City", "Postal Code", "Country"]
        vals = [remove_non_ascii(i).strip().split("\r")[0] 
                for i in data["Headquarters"].split("  ") if i.strip() != ""]
        _data = dict(zip(cols, vals))
        _data["website"] = data["Website"]
        _data["State"] = us.states.lookup(_data["City"].split(", ")[-1]).name
        _data["City"] = _data["City"].split(",")[0]
        _data["Phone"] = data["Phone"]
        _data["domain"] = "{0}.{1}".format(tldextract.extract(_data["website"]).domain, 
                                           tldextract.extract(_data["website"]).tld)
        return _data
        
    def _employee_df(self, data):
        qry = {"where":json.dumps({"domain": data["domain"]})}
        r = Parse().get("EmailPattern", qry).json()["results"][0]
        pattern = r["company_email_pattern"][0]["pattern"]
        
        employees = Parse().get("CompanyEmployee", {"limit":1000}).json()["results"]
        employees = pd.DataFrame(employees)
        employees.drop_duplicates("name").shape
        
        employees["First Name"] = [name.split(" ")[0] for name in employees.name]
        employees["Last Name"] = [name.split(" ")[-1] for name in employees.name]
        employees["first_name"] = [name.split(" ")[0] for name in employees.name]
        employees["last_name"] = [name.split(" ")[-1] for name in employees.name]
        employees["first_initial"] = [name.split(" ")[0][0] for name in employees.name]
        employees["last_initial"] = [name.split(" ")[-1][0] for name in employees.name]
        employees["Job Title"] = [title for title in employees.title]
        
        employees = employees.join(pd.DataFrame([data]*employees.shape[0]))
        employees["Email Address"] = [pystache.render(pattern, row.to_dict()).lower().replace("'","")
                                      for i, row in employees.iterrows()]
        employees["Company"] = company_name
        # upload csv to 
        employees = employees[['Email Address', 'First Name','Last Name' ,'Job Title',
                               'Address Line1','City','Company','Country',
                               'Phone','Postal Code','State']]
        employees = employees.drop_duplicates()
        employees.to_csv("/tmp/emp.csv",index=False,encoding="utf-8")


