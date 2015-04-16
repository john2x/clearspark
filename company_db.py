from bs4 import BeautifulSoup
import requests
from google import Google
from google import Crawlera
import tldextract
from crawl import *
import urllib 
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd

class GlassDoor:
    def _signal(self, link, api_key=""):
        html = Google().cache(link)
        info = self._html_to_dict(html)
        posts = self._reviews(html)
        CompanyInfoCrawl()._persist(info, "glassdoor", api_key)
        for post in posts:
          CompanyExtraInfoCrawl()._persist(post, "glassdoor_reviews", api_key)

    def _recent(self):
        df = Google().search("site:glassdoor.com/reviews", period="h")
        for link in df.link:
            q.enqueue(GlassDoor()._signal, link)

    def _company_profile(self, name, api_key=""):
        df = Google().search('site:glassdoor.com/overview {0}'.format(name))
        if df.empty: return CompanyInfoCrawl()._persist({'company_name': name}, "glassdoor", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "glassdoor", api_key)
    
    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:glassdoor.com/overview {0}'.format(name))
        if df.empty: return CompanyInfoCrawl()._persist({'company_name': name}, "glassdoor", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val = self._rename_vars(val)
            if "domain" not in val.keys(): continue
            if val["domain"] != domain: continue
            val["domain_search"] = True
            val['company_name'] = name
            val["domain"] = domain
            CompanyInfoCrawl()._persist(val, "glassdoor", api_key)
            break

    def _reviews(self, domain, api_key="", name=""):
        df = Google().search('site:glassdoor.com/reviews {0}'.format(name))
        url = df.ix[0].link
        r = BeautifulSoup(Google().cache(url))
        rating = r.find('div',{'class':'ratingNum'})
        rating = rating.text if rating else ""
        # TODO - awards
        reviews = pd.DataFrame()
        for review in r.find_all('li',{'class':'empReview'}):
            pros = review.find('p',{'class':'pros'})
            cons = review.find('p',{'class':'cons'})
            extra = review.find('p',{'class':'notranslate'})
            summary = review.find('span',{'class':'summary'})
            date = review.find('time',{'class':'date'})
            vals = [pros, cons, extra, summary, date]
            cols = ["pros", "cons", "extra", "summary", "date"]
            vals = [val.text.strip() for val in vals]
            reviews = reviews.append(dict(zip(cols, vals)),ignore_index=True) 
        print reviews
        data = {'data': reviews.to_dict('r'), 'company_name':name}
        data['api_key'] = api_key
        data['domain'] = domain
        CompanyExtraInfoCrawl()._persist(data, "glassdoor_reviews", api_key) 

    def _html_to_dict(self, url):
        r = BeautifulSoup(Google().cache(url))
        logo = r.find('div',{'class':'logo'})
        if logo:
          logo = logo.find('img')
          logo = logo['src'] if logo else ""
        else:
          logo = ""
        #website = r.find('span',{'class':'hideHH'}).text
        info = r.find('div',{'id':'EmpBasicInfo'})
        if info:
            info = info.find_all('div',{'class':'empInfo'})
        else:
            return {}
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
        if "website" in info.keys():
            tld = tldextract.extract(info["website"])
            _info['domain'] = "{}.{}".format(tld.domain, tld.tld)
        if "headquarters" in _info.keys():
            _info['address'] = _info['headquarters']
            del _info['headquarters']
        return _info

class BusinessWeek:
    def _domain_search(self, domain, api_key="", name=""):
        qry = 'site:businessweek.com/research {0} inurl:snapshot'.format(name)
        df = Google().search(qry)
        if df.empty: 
          return CompanyInfoCrawl()._persist({'company_name':name}, "businessweek", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val['company_name'] = name
            if "domain" not in val.keys(): continue
            if val["domain"] == domain:
                val["domain_search"] = True
                CompanyInfoCrawl()._persist(val, "businessweek", api_key)
                break

    def _company_profile(self, name, api_key=""):
        qry = 'site:businessweek.com/research {0} inurl:snapshot'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "businessweek", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "businessweek", api_key)

    def _html_to_dict(self, url):
        co = BeautifulSoup(Crawlera().get(url).text)
        name = co.find('span', {'itemprop':'name'})
        description = co.find('p', {'itemprop':'description'})
        address = co.find('div', {'itemprop':'address'})
        phone = co.find('div', {'itemprop':'telephone'})
        website = ""#co.find('div',{'id':'detailsContainer'}).find('a')
        # TODO - figure out why this is not working

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
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "forbes", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "forbes", api_key)

    def _domain_search(self, domain, api_key="", name=""):
        qry = 'site:forbes.com/companies {0}'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "forbes", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val = self._rename_vars(val)
            val['company_name'] = name
            if "domain" not in val.keys(): continue
            if val["domain"] != domain: continue
            val["domain_search"] = True
            CompanyInfoCrawl()._persist(val, "forbes", api_key)
            break

    def _html_to_dict(self, url):
        bs = BeautifulSoup(Crawlera().get(url).text)
        info = bs.find('div',{'class':'ataglanz'})
        if info:
          info = info.text.split('\n')
        else:
          return {}
        info = dict([i.strip().split(': ') for i in info  if ":" in i])
        logo = bs.find('div', {'class':'profileLeft'}).find('img')['src']
        info['logo'] = logo
        info['description'] = bs.find('p', {'id':'bio'}).text
        info['name'] = bs.find('hgroup').text
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        info = dict(zip([key.lower() for key in info.keys()], info.values()))
        if "industry" in info.keys():
          info['industry'] = [info['industry']]
        if "employees" in info.keys():
          info['employee_count'] = info['employees'].replace(',','')
          info['employee_count'] = info['employee_count'].replace('.','')
          info["employee_count"] = int(info["employee_count"])
          del info['employees']
        info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        if 'ceo' in info.keys(): del info['ceo']
        if 'founders' in info.keys(): del info['founders']
        if "country" in info.keys(): del info['country']
        info['address'] = info['headquarters']
        del info['headquarters']
        return info

class Crunchbase:
    def _company_profile(self, name, api_key=""):
        qry = 'site:crunchbase.com/organization {0}'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "crunchbase", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        CompanyInfoCrawl()._persist(val, "crunchbase", api_key)

    def _domain_search(self, domain, api_key="", name=""):
        qry = 'site:crunchbase.com/organization {0}'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "crunchbase", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val = self._rename_vars(val)
            val['company_name'] = name
            if "domain" not in val.keys(): continue
            if val["domain"] != domain: continue
            val["domain_search"] = True
            CompanyInfoCrawl()._persist(val, "crunchbase", api_key)
            break

    def _html_to_dict(self, url):
        cb = Google().cache(url)
        cb = BeautifulSoup(cb)
        info = cb.find('div',{'class':'info-card-content'})
        if info:
          info = info.find('div',{'class':'definition-list'})
        else:
          return {}
        vals = [label.text for label in info.find_all('dd')]
        cols = [label.text[:-1].lower() for label in info.find_all('dt')]
        info = dict(zip(cols, vals))
        info['logo'] = cb.find('img')['src']
        info['name'] = cb.find('h1').text
        info['handle'] = url
        return info

    def _rename_vars(self, info):
        _info = info
        if 'categories' in info.keys():
            _info['industry'] = [info['categories']]
            del info['categories']
        else:
            _info['industry'] = []
        if "website" in info.keys():
          tld = tldextract.extract(info['website'])
          _info['domain'] = "{}.{}".format(tld.domain, tld.tld)
        if "headquarters" in _info.keys():
          _info['address'] = _info['headquarters']
          del _info['headquarters']
        return _info

class Hoovers:
    def _company_profile(self, name, api_key=""):
        qry = 'site:http://www.hoovers.com {0} inurl:company-profile'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "hoovers", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val = self._rename_vars(val)
        val['company_name'] = name
        val["domain_search"] = True
        CompanyInfoCrawl()._persist(val, "hoovers", api_key)

    def _domain_search(self, domain, api_key="", name=""):
        qry = 'site:http://www.hoovers.com {0} inurl:company-profile'.format(name)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "hoovers", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val = self._rename_vars(val)
            val['company_name'] = name
            if "domain" not in val.keys(): continue
            if val["domain"] != domain: continue
            val["domain_search"] = True
            CompanyInfoCrawl()._persist(val, "hoovers", api_key)
            break

    def _html_to_dict(self, _url):
        url = _url
        bs = BeautifulSoup(Crawlera().get(url).text)
        name = bs.find('h1',{'itemprop':'name'})
        name = name.text.split('Company ')[0] if name else ""
        telephone = bs.find('span',{'itemprop':'telephone'})
        telephone = telephone.text if telephone else ""
        try:
          address = bs.find('p',{'itemprop':'address'}).text.split(telephone)[0].strip()
        except:
          address = ""
        url = bs.find('p',{'itemprop':'address'})
        url = url.find('a') if url else ""
        url = url.text if url else ""
        cols = ["name","phone","address","website"]
        vals = [name, telephone, address, url]
        info = dict(zip(cols , vals))
        info['handle'] = _url
        if "website" in info.keys():
          tld = tldextract.extract(info["website"])
          info['domain'] = "{}.{}".format(tld.domain, tld.tld)
        return info

    def _rename_vars(self, info):
        _info = info
        _info['domain'] = "{}.{}".format(tldextract.extract(info['website']).domain, tldextract.extract(info['website']).tld)
        return _info

class Yelp:
    def _company_profile(self, company_name, location="", api_key=""):
        df = Google().search('site:yelp.com {0}'.format(company_name))
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':company_name}, "yelp", api_key)
        url = df.ix[0].link
        val = self._html_to_dict(url)
        val['company_name'] = company_name
        CompanyInfoCrawl()._persist(val, "yelp", api_key)

    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:yelp.com {0}'.format(name))
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, "yelp", api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val['company_name'] = name
            if "domain" not in val.keys(): continue
            if val["domain"] != domain: continue
            val["domain_search"] = True
            CompanyInfoCrawl()._persist(val, "yelp", api_key)
            break

    def _remove_non_ascii(self, text):
        return ''.join(i for i in text if ord(i)<128)

    def _html_to_dict(self, url):
        r = Crawlera().get(url).text
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
            tld = tldextract.extract(self._remove_non_ascii(data["website"]))
            data['domain'] = "{}.{}".format(tld.domain, tld.tld)
        data["handle"] = url
        return data

class YellowPages:
    def _company_profile(self, company_name, location="", api_key=""):
        qry = '{0} {1} inurl:yellowpages inurl:/bus/'.format(company_name, location)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':company_name}, 'yellowpages', api_key)
        val = self._html_to_dict(df.ix[0].link)
        val['company_name'] = company_name
        print "YellowPages", val
        CompanyInfoCrawl._persist(val, 'yellowpages', api_key)

    def _domain_search(self, domain, api_key="", name="",location=""):
        qry = '{0} {1} inurl:yellowpages inurl:/bus/ -inurl:search'.format(name, location)
        df = Google().search(qry)
        if df.empty: return CompanyInfoCrawl()._persist({'company_name':name}, 'yellowpages', api_key)
        for count, url in enumerate(df.link):
            if count > 9: break
            val = self._html_to_dict(url)
            val['company_name'] = name
            print "YellowPages", val
            if "domain" not in val.keys(): continue
            if val["domain"] == domain: continue
            val["domain_search"] = True
            CompanyInfoCrawl._persist(val, 'yellowpages', api_key)
            break

    def _html_to_dict(self, url):
        #r = requests.get(url).text
        r = Crawlera().get(url).text
        print url
        try:
            company_name = BeautifulSoup(r).find('h1',{'itemprop':'name'})
            company_name = company_name.find('strong').text
        except:
            return {"handle": url}
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

class Indeed:
  def _company_profile(self, name, api_key=""):
      df = Google().search('site:indeed.com/cmp {0}'.format(name))
      if df.empty: 
          return CompanyInfoCrawl()._persist({'company_name': name}, 
                                             "indeed", api_key)
      df['_name'] = [i.split("Careers and Employment")[0].strip() 
                     for i in df.link_text]
      df["score"] = [fuzz.ratio(b, name) for b in df._name]
      df = df[df.score > 70]
      df = df.reset_index().drop('index',1)
      df = df.sort('score',ascending=False)
      if df.empty: 
        return CompanyInfoCrawl()._persist({'company_name': name},"indeed",api_key)
      else:
        url = df.ix[0].link
      val = self._html_to_dict(url)
      print "name"
      val["handle"] = url
      val['company_name'] = name
      print val
      CompanyInfoCrawl()._persist(val, "indeed", api_key)

  def _domain_search(self, domain, api_key="", name=""):
      df = Google().search('site:indeed.com/cmp {0}'.format(domain))
      if df.empty: 
          return CompanyInfoCrawl()._persist({'company_name': name}, "indeed", api_key)
      for count, url in enumerate(df.link):
          if count > 9: break
          val = self._html_to_dict(url)
          val['company_name'] = name
          if "domain" not in val.keys(): continue
          if val["domain"] == domain:
              val["domain_search"] = True
              CompanyInfoCrawl()._persist(val, "indeed", api_key)

  def _html_to_dict(self, url):
    r = Crawlera().get(url).text
    try:
      name = BeautifulSoup(r).find('h1',{'id':'company_name'}).text
    except:
      return {}
    desc= BeautifulSoup(r).find('span',{'id':'desc_short'})
    desc= desc.text if desc else ""
    data  = {'name':name, 'description':desc}
    content = BeautifulSoup(r).find_all('td',{'class':'metadata_content'})
    links = []
    for c in content:
        links = links + c.find_all('a')
    for i in links:
        if "website" in i.text:
            print i['href']
            website = urllib.unquote(i['href']).split('=')[1]
            website = website.split('?')[0].split('&')[0]
            domain = "{}.{}".format(tldextract.extract(website).domain, 
                                    tldextract.extract(website).tld)
            data["website"] = website
            data["domain"] = domain
    return data

  def _search_results_html_to_df(self, html_arr):
    '''
        Input  : Indeed.com raw_html
        Output : DF with relevant listing info
    '''
    jobs = pd.DataFrame(columns=['job_title','company_name','location','date','summary'])
    for count, page in enumerate(html_arr):
        soup = BeautifulSoup(page)
        tmp = pd.DataFrame()

        for row in soup.findAll('div',{'class':'row'}):
            job_title = row.find(attrs={'class':'jobtitle'}).text.strip() if row.find(attrs={'class':'jobtitle'}) else ""
            company = row.find('span',{'class':'company'}).text if row.find('span',{'class':'company'}) else ""
            location = row.find('span',{'class':'location'}).text if row.findAll('span',{'class':'location'}) else ""
            date = row.find('span',{'class':'date'}).text if row.findAll('span',{'class':'date'}) else ""
            summary = row.find('span',{'class':'summary'}).text.strip() if row.findAll('span',{'class':'summary'}) else ""

            cols = ['job_title','company_name','location','date','summary']

            tmp = tmp.append(dict(zip(cols,[job_title,company,location,date,summary])), ignore_index=True)
        jobs = jobs.append(tmp)
    return jobs
