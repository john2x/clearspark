from bs4 import BeautifulSoup
import requests
from google import Google
import tldextract
from crawl import CompanyInfoCrawl

class GlassDoor:
    def _company_profile(self, name, api_key=""):
        df = Google().search('site:glassdoor.com/overview {0}'.format(name))
        if df.empty: CompanyInfoCrawl()._persist({}, "glassdoor", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "glassdoor", api_key)

    def _reviews(self, name):
        ''' '''

    def _html_to_dict(self, url):
        r = BeautifulSoup(Google().cache(url))
        logo = r.find('div',{'class':'logo'}).find('img')['src']
        #website = r.find('span',{'class':'hideHH'}).text
        info = r.find('div',{'id':'EmpBasicInfo'}).find_all('div',{'class':'empInfo'})
        info = dict([[i.find('strong').text.lower().strip(), i.find('span').text.strip()] for i in info])
        info['name'] = r.find('div',{'class':'header'}).find('h1').text
        info['description'] = r.find('p',{'id':'EmpDescription'})
        info['description'] = info['description'].text if info['description'] else ""
        info['logo'] = logo
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        _info = info
        _info['industry'] = [info['industry']]
        _info['headcount'] = info['size'].replace(' to ', '-').split(' ')[0]
        del _info['size']
        _info['type'] = info['type'].split(' - ')[-1]
        _info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        _info['address'] = _info['headquarters']
        del _info['headquarters']
        return _info

class BusinessWeek:
    def _company_profile(self, name, api_key=""):
        qry = 'site:businessweek.com/research {0} inurl:snapshot'.format(name)
        df = Google().search(qry)
        if df.empty: CompanyInfoCrawl()._persist({}, "businessweek", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "businessweek", api_key)

    def _html_to_dict(self, url):
        co = BeautifulSoup(requests.get(url).text)
        name = co.find('span', {'itemprop':'name'})
        description = co.find('p', {'itemprop':'description'})
        address = co.find('div', {'itemprop':'address'})
        phone = co.find('div', {'itemprop':'telephone'})
        website = co.find('div',{'id':'detailsContainer'}).find('a')

        _vars = [name, description, address, phone, website]
        _vars = [var.text.strip() if var else "" for var in _vars]
        labels = ["name", "description", "address", "phone", "website"]
        print website
        data = dict(zip(labels, _vars))
        if data["website"] != "":
            data['domain'] = "{}.{}".format(tldextract.extract(data["website"]).domain, tldextract.extract(data["website"]).tld)
        data['handle'] = url
        return data

class Forbes:
    def _company_profile(self, name, api_key=""):
        qry = 'site:forbes.com/companies {0}'.format(name)
        df = Google().search(qry)
        if df.empty: CompanyInfoCrawl()._persist({}, "forbes", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "forbes", api_key)

    def _html_to_dict(self, url):
        bs = BeautifulSoup(requests.get(url).text)
        info = bs.find('div',{'class':'ataglanz'}).text.split('\n')
        info = dict([i.strip().split(': ') for i in info  if ":" in i])
        logo = bs.find('div', {'class':'profileLeft'}).find('img')['src']
        info['logo'] = logo
        info['description'] = bs.find('p', {'id':'bio'}).text
        info['name'] = bs.find('hgroup').text
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        info = dict(zip([key.lower() for key in info.keys()], info.values()))
        info['industry'] = [info['industry']]
        info['employee_count'] = int(info['employees'])
        del info['employees']
        info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        if 'ceo' in info.keys(): del info['ceo']
        if 'founders' in info.keys(): del info['founders']
        del info['country']
        info['address'] = info['headquarters']
        del info['headquarters']
        return info


class Crunchbase:
    def _company_profile(self, name, api_key=""):
        qry = 'site:crunchbase.com/organization {0}'.format(name)
        df = Google().search(qry)
        if df.empty: CompanyInfoCrawl()._persist({}, "crunchbase", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "crunchbase", api_key)

    def _html_to_dict(self, url):
        cb = Google().cache(url)
        cb = BeautifulSoup(cb)
        info = cb.find('div',{'class':'info-card-content'})
        info = info.find('div',{'class':'definition-list'})
        vals = [label.text for label in info.find_all('dd')]
        cols = [label.text[:-1].lower() for label in info.find_all('dt')]
        info = dict(zip(cols, vals))
        info['logo'] = cb.find('img')['src']
        info['name'] = cb.find('h1').text
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        _info = info
        _info['industry'] = [info['categories']]
        del info['categories']
        _info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        _info['address'] = _info['headquarters']
        del _info['headquarters']
        return _info

class Hoovers:
    def _company_profile(self, name, api_key=""):
        qry = 'site:http://www.hoovers.com {0} inurl:company-profile'.format(name)
        df = Google().search(qry)
        if df.empty: CompanyInfoCrawl()._persist({}, "hoovers", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "hoovers", api_key)

    def _html_to_dict(self, url):
        bs = BeautifulSoup(requests.get(url).text)
        name = bs.find('h1',{'itemprop':'name'}).text.split('Company ')[0]
        telephone = bs.find('span',{'itemprop':'telephone'}).text
        address = bs.find('p',{'itemprop':'address'}).text.split(telephone)[0].strip()
        url = bs.find('p',{'itemprop':'address'}).find('a').text
        cols = ["name","phone","address","website"]
        vals = [name, telephone, address, url]
        info = dict(zip(cols , vals))
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        _info = info
        _info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        return _info

class Yelp:
    def _company_profile(self, company_name, location="", api_key=""):
        df = Google().search('site:yelp.com {0}'.format(company_name))
        if df.empty: CompanyInfoCrawl()._persist({}, "yelp", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val['company_name'] = company_name
        print "Yelp", val
        CompanyInfoCrawl()._persist(val, "yelp", api_key)

    def _html_to_dict(self, url):
        r = requests.get(url).text
        company_name = BeautifulSoup(r).find('h1', {'class':'biz-page-title'})
        industry = BeautifulSoup(r).find('span', {'class':'category-str-list'})
        address = BeautifulSoup(r).find('address', {'itemprop':'address'})
        phone = BeautifulSoup(r).find('span', {'itemprop':'telephone'})
        website = BeautifulSoup(r).find('div', {'class':'biz-website'})
        website = website.find('a') if website else None

        _vars = [company_name, industry, address, phone, website]
        _vars = [var.text.strip() if var else "" for var in _vars]
        labels = ["name", "industry","address","phone","website"]
        data = dict(zip(labels, _vars))
        data["industry"] = [data["industry"]]
        print data
        if data["website"] != "":
            data['domain'] = "{}.{}".format(tldextract.extract(data["website"]).domain, tldextract.extract(data["website"]).tld)
        data["handle"] = url
        return data

class YellowPages:
    def _company_profile(self, company_name, location="", api_key=""):
        qry = '{0} {1} inurl:yellowpages inurl:/bus/'.format(company_name, location)
        df = Google().search(qry)
        if df.empty: CompanyInfoCrawl._persist({}, 'yellowpages', api_key)
        val = self._html_to_dict(df.ix[0].link)
        val['search_qry'] = company_name
        print "YellowPages", val
        CompanyInfoCrawl._persist(val, 'yellowpages', api_key)
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
        company["handle"] = url
        return company
