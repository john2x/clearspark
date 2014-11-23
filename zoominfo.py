from splinter import Browser
from bs4 import BeautifulSoup
import time
import pandas as pd
import difflib
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import requests
import re
import li

class Zoominfo:
    def _clean(name):
        name = name.split('(')[0]
        name = name.split(',')[0]
        name = name.split('\u')[0]
        name = re.split('[^a-zA-Z\d\s\-]', name)[0]
        return name

    def _browser():
        url = "http://www.zoominfo.com/s/#!search/company"
        #browser = Browser('chrome')
        browser = Browser('phantomjs')
        browser.visit(url)
        return browser

    def _fill_variables(self, ):
        ''' '''

    def search(industry=False,locale=False,name=False,employees=False,revenue=False):
        ''' Search '''
        browser = self._browser()
        self._fill_variables()

    def _zoominfo(name):
        browser = self._browser()
        if name: browser.find_by_name('companyName').first.fill(name)
        if industry: browser.find_by_name('industryKeywords').first.fill(industry)
        if locale: browser.find_by_name('address').first.fill(locale)
        
        while not browser.find_by_css('div.big').visible:
            if browser.is_text_present("No Results Found."): return "nope"
            time.sleep(0.2)
        return browser.html

    def _zoominfo_html_to_df(self, html):
        ''' Parse Zoominfo Search Results Into DF '''
        the_info = pd.DataFrame()
        results = BeautifulSoup(html).find('table',{'id':'resultGroup'})
        results = results.findAll('tr')
        for result in results:
            co = results.find('td',{'class':'name'})
            if co == None: continue
            name = co.find('a').text
            website = co.findAll('a')[-1]
            location = co.find('span',{'class':'companyAddress'})
            revenue = co.find('span',{'class':'revenueText'})
            employee_count = co.find('span',{'class':'employeeCount'})
            description = results.find('td',{'class':'description'})
            # add domain
            # change variables to parse db names

            columns = ['name','websiteUrl','website','city','revenue',
                       'headcount','description'] 
            values = [name, website, website, location, revenue, 
                      employee_count, description]
            values = [val.text if val else "" for val in values]
            the_info = the_info.append(dict(zip(columns,values)), ignore_index=True)

        # Add Check for websiteUrl must be a proper domain
        return the_info

    def _get_best_match(company_name, df):
        similar = difflib.get_close_matches(company_name, 
                                            [name for name in df.name])
        if len(similar) and process.extract(company_name, similar)[0][1] > 80:
            zoominfo_profile_name = process.extract(company_name, similar)[0][0]
            for i, zoominfo_profile in df.iterrows():
                if zoominfo_profile['name'] == zoominfo_profile_name:
                    return zoominfo_profile
        return "not found"

    def search(company_name):
        ''' 
          Input: Name and Possibly Location, Parse Object ObjectId
          Output: Update Parse Object
        '''
        name = clean_name(company_name)
        zoominfo_html = get_html_results_from_zoominfo(name)
        if zoominfo_html == "nope": return "not found"
        zoominfo_df = parse_zoominfo_html_into_df(zoominfo_html) 
        best_match = get_best_match(name, zoominfo_df)
        return best_match

        # Get Email
        # Update Parse With New Website and Phone Number
        # r = requests.put('https://api.parse.com/1/classes/Prospects/'+objectId, 
        #                 headers=headers, params=json.dumps(zoominfo_profile))
