from splinter import Browser
from bs4 import BeautifulSoup
import time
import pandas as pd
import difflib
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
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
            phone = result.find('span',{'class':'companyContactNo'})
            # change variables to parse db names

            columns = ['company_name','website','domain','city','revenue',
                       'company_size','description', 'phone'] 
            values = [name, website, domain, location, revenue, 
                      employee_count, description, phone]
            values = [val.text if val else "" for val in values]

            info = dict(zip(columns, values))
            info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
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

