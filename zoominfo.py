from splinter import Browser
from bs4 import BeautifulSoup
import time
import pandas as pd
import difflib
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from social import CompanyInfoCrawl
from google import Google
import requests
import tldextract
import re
import li

class Zoominfo:
    def _clean(self, name):
        name = name.split('(')[0]
        name = name.split(',')[0]
        name = name.split('\u')[0]
        name = re.split('[^a-zA-Z\d\s\-]', name)[0]
        return name

    def _browser(self):
        url = "http://www.zoominfo.com/s/#!search/company"
        #browser = Browser('chrome')
        browser = Browser('phantomjs')
        browser.visit(url)
        return browser

    def _fill_variables(self, ):
        ''' lol - lmao'''

    def __search(industry=False,locale=False,name=False,employees=False,revenue=False):
        ''' Search '''
        browser = self._browser()
        self._fill_variables()

    def _fill_variables(self, name, industry=False, locale=False):
        browser = self._browser()
        if name: browser.find_by_name('companyName').first.fill(name)
        if industry: browser.find_by_name('industryKeywords').first.fill(industry)
        if locale: browser.find_by_name('address').first.fill(locale)
        
        while not browser.find_by_css('div.big').visible:
            if browser.is_text_present("No Results Found."): return "nope"
            time.sleep(0.2)
        return browser.html

    def _zoominfo_search_html_to_df(self, html):
        ''' Parse Zoominfo Search Results Into DF '''
        the_info = pd.DataFrame()
        results = BeautifulSoup(html).find('table',{'id':'resultGroup'})
        results = results.findAll('tr')
        for result in results:
            co = result.find('td',{'class':'name'})
            if co == None: continue
            name = co.find('a')
            website = co.findAll('a')[-1] if len(co.findAll('a')) != 1 else ""
            domain = co.findAll('a')[-1]
            location = co.find('span',{'class':'companyAddress'})
            revenue = co.find('span',{'class':'revenueText'})
            employee_count = co.find('span',{'class':'employeeCount'})
            description = result.find('td',{'class':'description'})
            phone = BeautifulSoup(html).find('span',{'class':'companyContactNo'})
            # change variables to parse db names

            columns = ['company_name','website','domain','city','locale','revenue',
                       'headcount','description', 'phone'] 
            values = [name, website, domain, location, location, revenue, 
                      employee_count, description, phone]
            values = [val.text if val else "" for val in values]

            info = dict(zip(columns, values))
            info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
            info['source'] = "zoominfo"
            the_info = the_info.append(info, ignore_index=True)

        # Add Check for websiteUrl must be a proper domain
        return the_info

    def _get_best_match(self, company_name, df):
        similar = difflib.get_close_matches(company_name, 
                                            [name for name in df.company_name])
        if len(similar) and process.extract(company_name, similar)[0][1] > 80:
            zoominfo_profile_name = process.extract(company_name, similar)[0][0]
            for i, zoominfo_profile in df.iterrows():
                if zoominfo_profile['company_name'] == zoominfo_profile_name:
                    return zoominfo_profile.to_dict()
        return "not found"

    def _company_profile(self, company_name):
        qry = 'site:zoominfo.com/c/ {0}'.format(company_name)
        google_df = Google().search(qry)
        url = google_df.ix[0].link
        print "ZOOMINFO URL", url
        html = Google().ec2_cache(url)
        print html
        html = requests.get(url).text
        html = self._remove_non_ascii(html)
        print html
        zoominfo = self._cache_html_to_df(html)
        CompanyInfoCrawl()._persist(zoominfo)

    def _remove_non_ascii(self, text):
        return ''.join(i for i in text if ord(i)<128)

    def _cache_html_to_df(self, html):
        company = BeautifulSoup(html)
        title = company.find('div',{'class':'companyTitle'})
        description = company.find('div',{'class':'companyDescription'})
        revenue = company.find('div',{'class':'companyRevenue'})
        address = company.find('div',{'class':'companyAddress'})
        employee_count = company.find('p',{'class':'companyEmployeeCountText'})
        website = company.find('div',{'class':'website'})
        phone = company.find('span',{'class':'hq'})
        
        data = [title, description, revenue, address, employee_count,
                website, phone]
        columns = ["title", "description", "revenue", "address",
                   "address","employee_count","website","phone"]
        data = [val.text if val else "" for val in data]
        data = dict(zip(columns, data))
        data["domain"] = "{}.{}".format(tldextract.extract(data["website"]).domain,
                                        tldextract.extract(data["website"]).tld)
        try:
          data['logo'] = company.find('img',{'class':'companyLogo'})['src']
        except:
          data['logo'] = ""
        data["source"] = "zoominfo"
        print data
        return data

    def _profile_to_df(self, html):
        ''' lol '''
        title = company.find('h1',{'class':'companyName'})
        description = company.find('span',{'class':'companyDesc'})
        revenue = company.find('span',{'class':'revenueText'})
        address = company.find('span',{'class':'companyAddress'})
        employee_count = company.find('span',{'class':'employeeCount'})
        website = company.find('div',{'class':'website'})
        phone = company.find('span',{'class':'companyContactNo'})
        data = [title, description, revenue, address, employee_count,
                website, phone, url, logo]
        columns = ["title", "description", "revenue", "address",
                   "address","employee_count","website","phone", "url", "logo"]
        data = [val.text if val else "" for val in data]
        data = dict(zip(columns, data))
        data["domain"] = "{}.{}".format(tldextract.extract(data["website"]).domain,
                                        tldextract.extract(data["website"]).tld)
        data["source"] = "zoominfo"
        try:
          data['logo'] = company.find('img',{'class':'companyImgLogo'})['src']
        except:
          data['logo'] = ""
        print data
        return data

    def _search(self, company_name):
        name = self._clean(company_name)
        zoominfo_html = self._fill_variables(company_name)
        # click first
        # companyResultsName
        # scrape

    def search(self, company_name):
        ''' 
          Input: Name and Possibly Location, Parse Object ObjectId
          Output: Update Parse Object
        '''
        name = self._clean(company_name)
        zoominfo_html = self._fill_variables(company_name)
        #zoominfo_html = get_html_results_from_zoominfo(name)
        if zoominfo_html == "nope": return "not found"
        zoominfo_df = self._zoominfo_search_html_to_df(zoominfo_html) 
        best_match = self._get_best_match(name, zoominfo_df)
        return best_match

        # Get Email
        # Update Parse With New Website and Phone Number
        # r = requests.put('https://api.parse.com/1/classes/Prospects/'+objectId, 
        #                 headers=headers, params=json.dumps(zoominfo_profile))

