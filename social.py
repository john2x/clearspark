from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
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
        
class Yelp:
    def _company_profile(self, company_name, location=""):
        df = Google().search('site:yelp.com {0}'.format(company_name))
        '''
        for url in df.link:
            val = self._html_to_dict(url)
            if val != {}: break
        '''
        if not df.empty:
            url = df.ix[0].link
            val = self._html_to_dict(url)
            val['search_qry'] = company_name
            print "Yelp", val
            CompanyInfoCrawl()._persist(val)

    def _html_to_dict(self, url):
        r = requests.get(url).text
        company_name = BeautifulSoup(r).find('h1', {'class':'biz-page-title'})
        industry = BeautifulSoup(r).find('span', {'class':'category-str-list'})
        address = BeautifulSoup(r).find('span', {'itemprop':'streetAddress'})
        city = BeautifulSoup(r).find('span', {'itemprop':'addressLocality'})
        state = BeautifulSoup(r).find('span', {'itemprop':'addressRegion'})
        postal_code = BeautifulSoup(r).find('span', {'itemprop':'postalCode'})
        phone = BeautifulSoup(r).find('span', {'itemprop':'telephone'})
        website = BeautifulSoup(r).find('div', {'class':'biz-website'}).find('a')


        _vars = [company_name, industry, address, city, state, postal_code, phone, website, domain]
        _vars = [var.text.strip() if var else "" for var in _vars]
        labels = ["name", "industry","address","city","state","postal_code","phone","website", "domain"]
        data = dict(zip(labels, _vars))
        website = data['domain']
        data['domain'] = "{}.{}".format(tldextract.extract(website).domain, tldextract.extract(website).tld)
        return data

class YellowPages:
    def _company_profile(self, company_name, location=""):
        qry = '{0} {1} inurl:yellowpages inurl:/bus/'.format(company_name, location)
        df = Google().search(qry)
        if not df.empty:
            url = df.ix[0].link
            val = self._html_to_dict(url)
            val['search_qry'] = company_name
            print "YellowPages", val
            CompanyInfoCrawl._persist(val, 'yellowpages')
        '''
        for url in df.link:
            val = self._html_to_dict(url, domain)
            if val != {}: break
        '''

    def _html_to_dict(self, url):
        r = requests.get(url).text
        company_name = BeautifulSoup(r).find('h1',{'itemprop':'name'}).find('strong').text
        address = BeautifulSoup(r).find('h1',{'itemprop':'name'}).find('span').text
        city = BeautifulSoup(r).find('span',{'itemprop':'addressLocality'}).text
        state = BeautifulSoup(r).find('span',{'itemprop':'addressRegion'}).text
        postal_code = BeautifulSoup(r).find('span',{'itemprop':'postalCode'}).text
        description = BeautifulSoup(r).find('article',{'itemprop':'description'}).text.strip().replace('\nMore...','')
        logo = BeautifulSoup(r).find('figure').find('img')['src']
        website = BeautifulSoup(r).find('li',{'class':'website'}).find('a')['href'].split('gourl?')[-1]
        domain = "{}.{}".format(tldextract.extract(website).domain, tldextract.extract(website).tld)
        ''' Phone '''
        main = BeautifulSoup(r).find('li',{'class':'phone'}).find('strong',{'class':'primary'}).text
        numbers = BeautifulSoup(r).find('li',{'class':'phone'}).findAll('li')
        nums = [number.find('span').text for number in numbers]
        names = [number.text.split(number.find('span').text)[0] for number in numbers]
        numbers = dict(zip(names, nums))
        numbers['main'] = main

        _vars = [company_name, address, city, state, postal_code, description, logo, website, domain]
        labels = ["name","address","city","state","postal_code", "description", "logo", "website", "domain"]
        company = dict(zip(labels, _vars))
        company["numbers"] = numbers
        return company
