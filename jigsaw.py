from splinter import Browser
import time
import uuid
from companies import Companies
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
        job_1 = q.enqueue(GoogleSearch()._employees, company_name)
        job_2 = q.enqueue(LinkedinTitleDir()._search, company_name)
        jobs = [job_1, job_2]
        for job in jobs:
            RQueue()._meta(job, queue_name, "queue_name")
            RQueue()._meta(job, company_name, "company_name")

    def _upload_csv():
        ''' '''
        # collect
        # format columns
        # self.upload_csv()
