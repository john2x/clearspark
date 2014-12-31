from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
from crawl import CompanyInfoCrawl
import tldextract

class Twitter:
    def _company_profile(self, company_name, domain):
        df = Google().search('site:twitter.com {0}'.format(domain))
        for url in df.link:
            val = self._html_to_dict(url, domain)
            if val != {}: break
        CompanyInfoCrawl._persist(val)

    def _html_to_dict(self, url, domain):
        r = requests.get(url).text
        link = BeautifulSoup(r).find('span',{'class':'ProfileHeaderCard-urlText'}).text.strip()
        if domain not in link: return {}
        logo = BeautifulSoup(r).find('img',{'class':'ProfileAvatar-image '})['src']
        link = BeautifulSoup(r).find('h2',{'class':'ProfileHeaderCard-screenname'}).text.strip().lower()
        return {'logo':logo, 'twitter_handle':link, 'source':'twitter'}

class Facebook:
    def _company_profile(self, company_name, domain):
        df = Google().search('site:facebook.com {0}'.format(domain))
        for url in df.link:
            val = self._html_to_dict(url, domain)
            if val != {}: break
        CompanyInfoCrawl._persist(val)

    def _html_to_dict(self, url, domain):
        browser = Browser('phantomjs')
        browser.visit(url)
        if domain in browser.html: return {}
        logo = BeautifulSoup(browser.html).find('img',{'class':'profilePic'})['src']
        link = BeautifulSoup(browser.html).find('a',{'class':'profileLink'})['href']
        return {'logo':logo, 'facebook_handle':link, 'source':'facebook'}
        
