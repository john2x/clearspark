from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
from crawl import CompanyInfoCrawl
import tldextract

class Twitter:
    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:twitter.com {0}'.format(domain))
        for url in df.link:
            r = requests.get(url).text
            link = BeautifulSoup(r).find('span',{'class':'ProfileHeaderCard-urlText'}).text.strip()
            if domain not in link: continue
            val = self._html_to_dict(url, domain)
            break
        CompanyInfoCrawl()._persist(val, "twitter", api_key)

    def _company_profile(self, name, api_key=""):
        df = Google().search('site:twitter.com {0}'.format(name))
        url = df.link.tolist()[0]
        html = requests.get(url).text
        val = self._html_to_dict(html)
        CompanyInfoCrawl()._persist(val, "twitter", api_key)

    def _html_to_dict(self, html):
        logo = BeautifulSoup(html).find('img',{'class':'ProfileAvatar-image '})['src']
        link = BeautifulSoup(html).find('h2',{'class':'ProfileHeaderCard-screenname'}).text.strip().lower()
        link = "twitter.com/"+link.split('@')[0]
        name = BeautifulSoup(html).find('h1',{'class':'ProfileHeaderCard-name'}).text.strip().lower()
        # add company_name
        return {'logo':logo, 'handle':link, 'name':name}

class Facebook:
    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:facebook.com {0}'.format(domain))
        for url in df.link:
            #browser = Browser('phantomjs')
            #browser.visit(url)
            # html = browser.html
            html = Google().cache(url)
            if domain not in BeautifulSoup(html).text: continue
            val = self._html_to_dict(html)
            break
        CompanyInfoCrawl()._persist(val, "facebook", api_key)

    def _company_profile(self, name, api_key=""):
        df = Google().search('site:facebook.com {0}'.format(name))
        url = df.link.tolist()[0]
        html = Google().cache(url)
        #browser = Browser('phantomjs')
        #browser.visit(url)
        val = self._html_to_dict(html)
        CompanyInfoCrawl()._persist(val, "facebook", api_key)

    def _html_to_dict(self, html):
        html = BeautifulSoup(html)
        logo = html.find('img',{'class':'profilePic'})['src']
        link = html.find('a',{'class':'profileLink'})['href']
        name = html.find('span',{'itemprop':'name'}).text
        # add company_name
        return {'logo':logo, 'handle':link, 'name':name}
        
