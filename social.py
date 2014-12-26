from splinter import Browser
from parse import Parse

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
        for url in df.link:
            val = self._html_to_dict(url, domain)
            if val != {}: break
        CompanyInfoCrawl._persist(val)

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

        _vars = [company_name, industry, address, city, state, postal_code, phone, website]
        _vars = [var.text.strip() if var else "" for var in _vars]
        labels = ["company_name","industry","address","city","state","postal_code","phone","website"]
        return dict(zip(labels, _vars))

class YellowPages:
    def _company_profile(self, company_name, location=""):
        qry = '{0} {1} inurl:yellowpages inurl:/bus/'.format(company_name, location)
        df = Google().search()
        for url in df.link:
            val = self._html_to_dict(url, domain)
            if val != {}: break
        CompanyInfoCrawl._persist(val)

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
        labels = ["company_name","address","city","state","postal_code", "description", "logo", "website", "domain"]
        company = dict(zip(labels, _vars))
        company["numbers"] = numbers
        return company

class CompanyInfoCrawl:
    def _persist(self, data):
        crawl = Parse().create('CompanyInfoCrawl', data).json()
        crawl = Parse()._pointer('CompanyInfoCrawl', crawl['objectId'])
        company = Parse().get('Company', {'where':{'domain':data['domain']}}).json()
        if company:
            company = 'Company/'+company['objectId'], 
            data = {'__op':'AddUnique', "objects":[crawl]}
            print Parse().update(company, {'crawls': data}).json()
        else:
            print Parse().create(company, {'crawls':[crawl],'domain':data['domain']}).json()
