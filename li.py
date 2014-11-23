from linkedin import linkedin
import time
import toofr
import difflib
import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
#from rapportive import rapportive
import tldextract
import pandas as pd
import json
from google import Google
from bs4 import BeautifulSoup
import string

class Linkedin:
    def _parse_google_span_for_title_and_company(self, link_span):
        ''' Parse Google Search Linkedin Text For Current Position '''
        res = [span for span in link_span.split('...') if "Current." in span]
        if len(res):
            res = res[0].replace('\n',' ')
            pos = res.split('Current.')[1]
            pos = pos.split(' at ')
            print pos
            if len(pos) > 1:
                return {'pos':pos[0], 'company_qry':pos[1]}
        return "not found"

    def _linkedin_profile_from_name(self, company_name):
        '''   '''
        qry = company_name+' site:linkedin.com/company'
        google_results = Google().search(qry)
        if google_results.empty: return "not found"
        url = google_results.ix[0].url
        # scrape cache
        return url if "/company/" in url else "not found"

    def _company_profile(self, company_name):
        url = self._linkedin_profile_from_name(company_name)
        html = Google().cache(url)
        info = self._company_cache_html_to_df(html)
        return info

    def _create_linkedin_directory_urls_from_name(self, name):
        ''' name '''
        the_name = name.split(' ')[0]+'/'+name.split(' ')[-1]
        
        dir_urls = [url.replace('+','%2B') for url in dir_urls]
        dir_urls = ['http://www.linkedin.com/pub/dir/'+the_name] + dir_urls
        return dir_urls

    def _directory_search(self, name, description):
        qry = name+' "{0}" site:linkedin.com/pub/dir'.format(description)
        qry = filter(lambda x: x in string.printable, qry)
        results = Google().search(qry)
        count = 0
        while results.empty:
            print "trying again"
            results = Google().search(qry)
            count = count + 1
            if count > 2: break

        print results
        return results.url if not results.empty else []

    def _directory_scrape(self, url):
        html = Google().cache(url)
        return self._directory_html_to_df(html)

    def _directory_html_to_df(self, html):
        ''' Turn Into DF '''
        results = pd.DataFrame()
        if "404" not in html:
            people = BeautifulSoup(html).findAll('li',{'class':'vcard'})
            if people == None: return results
            
            for person in people:
                name = person.find('h2').find('a').text
                linkedin_url = person.find('h2').find('a')['href']
                description = person.find('dd',{'class':'title'})
                industry = person.find('span', {'class':'industry'})
                locale = person.find('span', {'class':'location'})
                try:
                  current = person.find('dd', {'class':'current-content'}).text
                except:
                  continue
                title = current.split(' at ')[0]
                company_name = current.split(' at ')[-1]
                #company_name = "" if title in company_name else company_name
                company_name = ""

                description = description.text.strip() if description else ""
                industry = industry.text.strip() if industry else ""
                locale = locale.text.strip() if locale else ""
                current = current.strip() if current else ""

                columns = ['name', 'description', 'industry', 'locale', 'pos', 
                           'company_name', 'linkedin_url']
                values = [name, description, industry, locale, title, 
                          company_name, linkedin_url]
                results = results.append(dict(zip(columns,values)), ignore_index=True)
        return results
    
    def _int_to_linkedin_company_size_string(company_size):
        if company_size == 1:
            company_info = "1 employee"
        elif company_size >= 2 and company_size <= 10:
            company_info = "2-10 employees"
        elif company_size >= 11 and company_size <= 50:
            company_info = "11-50 employees"
        elif company_size >= 51 and company_size <= 200:
            company_info = "51-200 employees"
        elif company_size >= 201 and company_size <= 500:
            company_info = "201-500 employees"
        elif company_size >= 501 and company_size <= 1000:
            company_info = "501-1000 employees"
        elif company_size >= 1001 and company_size <= 5000:
            company_info = "1001-5000 employees"
        elif company_size >= 5001 and company_size <= 10000:
            company_info = "5001-10000 employees"
        elif company_size < 10000:
            company_info = "10000+ employees"
        return company_info

    def _company_cache_html_to_df(self, html):
        company_info = pd.DataFrame()
        c = BeautifulSoup(html)
        try:
            cols = [i.find('h4').text
                    for i in c.find('dd',{'class','basic-info-about'}).findAll('li')]
            vals = [i.find('p').text.strip()
                    for i in c.find('dd',{'class','basic-info-about'}).findAll('li')]
            company_info = company_info.append(dict(zip(cols,vals)),ignore_index=True)

            company_info.columns = [col.replace(' ','_').strip().lower()
                                    for col in company_info.columns]
            # new code not in other methods in different file
            company_info['company_name'] = c.find('h1',{'class':'name'}).text.strip()
            if "company_size" not in company_info.columns:
                company_size = int(c.find('a',{'class':'employee-count'}).text)
                company_size = int_to_linkedin_company_size_string(company_size)
                company_info['company_size'] = company_size
            # domain
            website = company_info['website'].ix[0]
            domain = "{}.{}".format(tldextract.extract(website).domain, 
                                    tldextract.extract(website).tld)
            company_info['domain'] = domain
            return company_info
        except:
            return "not found"