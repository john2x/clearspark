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
from crawl import *
from company_db import *

class Linkedin:
    def _signal(self, link, api_key=""):
        html = Google().cache(link)
        #info = self._html_to_dict(html)
        posts = self._company_posts(html)
        '''
        CompanyInfoCrawl()._persist(info, "linkedin", api_key)
        for post in posts:
          CompanyExtraInfoCrawl()._persist(post, "linkedin_posts", api_key)
        '''

    def _recent(self):
        df = Google().search("site:linkedin.com/company", period="h")
        for link in df.link:
            q.enqueue(Linkedin()._signal, link)

    def _daily_news(self, domain, api_key="",  name=""):
        df = Google().search("site:linkedin.com/company {0}".format(domain))
        if df.empty: return 
        #for link in df.link:
        link = df.link.tolist()[0]
        print link
        html = Google().cache(link)
        posts = self._company_posts(html)
        #Linkedin()._signal(link, api_key)
        data = {"data":posts, "company_name":name, "domain":domain}
        CompanyExtraInfoCrawl()._persist(data, "linkedin_posts", api_key)

    def _pulse_posts():
        ''' '''

    def _company_posts(self, html, api_key=""):
        li = BeautifulSoup(html)
        posts = []
        for post in li.find_all("li",{"class":"feed-item"}):
            img = post.find("img")
            img = img["src"] if img else ""
            date = post.find("a", {"class":"nus-timestamp"}).text
            timestamp = Helper()._str_to_timestamp(date)
            post = post.find("span",{"class":"commentary"})
            try:
                link = [i.text for i in post.find_all("a")]
            except:
                continue
            data = {"img":img, "post":post.text, "link":link, "date":date, 
                    "timestamp":timestamp}
            #TODO - add timestamp
            posts.append(data)
        return posts

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

    def _google_df_to_linkedin_df(self, results):
        final = pd.DataFrame()
        if results.empty: return final
        final['name'] = [name.split('|')[0].strip().split(',')[0] 
                         for name in results.link_text]
        final['locale']  = [name.split('-')[0].strip() 
                            for name in results.title]
        final['company']  = [name.split(' at ')[-1].strip() 
                              if " at " in name else ""
                             for name in results.title]
        final['title']  = [name.split(' at ')[0].split('-')[-1].strip()
                             for name in results.title]
        final['linkedin_url'] = results.link
        return final

    def _linkedin_profile_from_name(self, company_name):
        qry = company_name+' site:linkedin.com/company'
        google_results = Google().search(qry)
        if google_results.empty: return "not found"
        url = google_results.ix[0].url
        # scrape cache
        return url if "/company/" in url else "not found"

    def _domain_search(self, domain, api_key="", name=""):
        qry = 'site:linkedin.com/company {0}'.format(domain)
        google_results = Google().search(qry)
        if google_results.empty: 
          data = {'company_name':name, "domain":domain}
          return CompanyInfoCrawl()._persist(data, 'linkedin', api_key)
        url = google_results.ix[0].url
        html = Google().cache(url)
        info = self._company_cache_html_to_df(html)
        if type(info) is str: 
            data = {'company_name':name, "domain":domain}
            return CompanyInfoCrawl()._persist(data, 'linkedin', api_key)
        info = json.loads(info.ix[0].to_json())
        info['company_name'] = name
        info['handle'] = url
        info["domain_search"] = True
        if info["domain"] == domain:
            CompanyInfoCrawl()._persist(info, 'linkedin', api_key)
            return info 

    def _company_profile(self, company_name, api_key):
        qry = company_name+' site:linkedin.com/company'
        print qry
        google_results = Google().search(qry)
        print google_results
        if google_results.empty: 
          return CompanyInfoCrawl()._persist({'company_name':company_name}, 'linkedin', api_key)
        url = google_results.ix[0].url
        html = Google().cache(url)
        print html
        print url
        info = self._company_cache_html_to_df(html)
        if type(info) is str: return CompanyInfoCrawl()._persist({'company_name':company_name}, 'linkedin', api_key)
        info = json.loads(info.ix[0].to_json())
        info['company_name'] = company_name
        info['handle'] = url
        CompanyInfoCrawl()._persist(info, 'linkedin', api_key)
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
                           'company_name', 'linkedin_url', 'source']
                values = [name, description, industry, locale, title, 
                          company_name, linkedin_url,'linkedin']
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
        #print c.find('dd',{'class','basic-info-about'}).text
        if True:
            cols = [i.find('h4').text
                    for i in c.find('dd',{'class','basic-info-about'}).findAll('li')]
            vals = [i.find('p').text.strip()
                    for i in c.find('dd',{'class','basic-info-about'}).findAll('li')]
            company_info = company_info.append(dict(zip(cols,vals)),ignore_index=True)
            company_info.columns = [col.replace(' ','_').strip().lower()
                                    for col in company_info.columns]
            company_info['description'] = c.find('div', {'class':'description'}).text.strip()
            # rename companies title columns
            img = c.find('div',{'class':'image-wrapper'}).find('img')['src']
            company_info['logo'] =  img
            # new code not in other methods in different file
            company_info['name'] = c.find('h1',{'class':'name'}).text.strip()
            company_info['employee_count'] = int(c.find('a',{'class':'employee-count'}).text.replace(',',''))
            for i in c.find_all("h3"):
                if i.find("a"):
                    url = i.find("a")["href"]
                    url = url.split("?")[-1]
                    args = dict([i.split("=") for i in url.split("&")])
                    if "f_CC" in args.keys():
                      url = "http://linkedin.com/company/{0}".format(args["f_CC"])
                    else:
                      url = "not found"
            company_info["linkedin_url"] = url

            if 'headquarters' in company_info.columns:
                company_info['address'] = company_info['headquarters']
                company_info.drop('headquarters', axis=1, inplace=True)
            if 'industry' in company_info.columns:
                company_info['industry'] = [[company_info['industry'].ix[0]] for i in range(company_info.shape[0])]

            website = company_info['website'].ix[0]
            domain = "{}.{}".format(tldextract.extract(website).domain, 
                                    tldextract.extract(website).tld)
            company_info['domain'] = domain
            company_info['source'] = "linkedin"
            company_info['headcount'] = company_info['company_size']
            company_info['headcount'] = company_info['headcount'].ix[0].split(' ')[0]

            if 'company_size' in company_info.columns:
                company_info.drop('company_size', axis=1, inplace=True)
            return company_info
        '''
        except Exception,e:
            print str(e)
            return "not found"
        '''
