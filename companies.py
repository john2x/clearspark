import requests
from bs4 import BeautifulSoup
import urllib
from fuzzywuzzy import fuzz
from webhook import Webhook
import pandas as pd
import tldextract
from parse import Parse, Prospecter
from google import Google
from li import Linkedin
import json
import re, string
import logging
from address import AddressParser, Address
import zoominfo
#from email_guess import EmailGuess
#from companies import Companies
from zoominfo import Zoominfo
import random
import toofr
from queue import RQueue
import time
''' RQ Setup '''
from social import *
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from company_db import *
from crawl import *
from rq import Queue
from worker import conn
from jigsaw import *
from company_score import *
import calendar
import arrow

q = Queue(connection=conn)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Companies:
    # TODO
    def _tags_describing_company():
        ''' '''

    def _description():
        ''' '''

    ''' Working '''
    def _company_blog(self, domain, api_key="", name=""):
        #TODO get blog url
        df = Google().search('inurl:blog site:{0}'.format(domain), 1)
        print df
        if df.empty: return
        df["count"] = [len(url) for url in df.link]
        df = df.reset_index().drop('index',1)
        df = df.drop('title', 1)
        url = df.sort('count').url.ix[0]
        df["timestamp"] = [i.split("...")[0].strip() for i in df.link_span]
        months = list(calendar.month_abbr)
        timestamps = []
        for _date in df.timestamp:
            try:
                num = months.index(_date.split(" ")[0])
            except:
                timestamps.append(0)
                continue
            _date = str(num)+" "+" ".join(_date.split(" ")[1:])
            try:
              timestamps.append(arrow.get(_date, "M D, YYYY").timestamp)
            except:
                if "day" in i:
                  num = int(i.split())
                  timestamps.append(arrow.utcnow().replace(days=num*-1).timestamp)
                else:
                  timestamps.append(0)
        df["timestamp"] = timestamps

        data = {'data': df.to_dict('r'), 'blog_url':url}
        data["domain"] = domain
        data["api_key"] = api_key
        data["company_name"] = name
        CompanyExtraInfoCrawl()._persist(data, "blog_data", api_key)

    def _hiring(self, domain, api_key="", company_name=""):
        # paginate
        jobs = "http://www.indeed.com/jobs?q={0}&sort=date".format(company_name)
        browser = Browser('phantomjs')
        browser.visit(jobs)
        pages = [browser.html]
        while "Next" in BeautifulSoup(browser.html).text:
            browser.find_by_css('.np')[-1].click()
            time.sleep(1)
            pages.append(browser.html)
        data = Indeed()._search_results_html_to_df(pages)
        #TODO - add timestamp
        data["name_score"]=[fuzz.ratio(company_name, i) for i in data.company_name]
        data = data[data.name_score > 70]
        date, timestamps = arrow.utcnow(), []
        for i in data.date:
            num = int(i.split(" ")[0].replace("+",""))
            if "hour" in i:
                timestamps.append(date.replace(hours=num*-1).timestamp)
            elif "minute" in i:
                timestamps.append(date.replace(minutes=num*-1).timestamp)
            elif "day" in i:
                timestamps.append(date.replace(days=num*-1).timestamp)
        data["timestamp"] = timestamps
        jobs = {"company_name":company_name}
        jobs["data"] = data.to_dict("r")
        jobs["domain"] = domain
        jobs["api_key"] = api_key
        CompanyExtraInfoCrawl()._persist(jobs, "hiring", api_key)

    def _press_releases(self, domain, api_key="", company_name=""):
        ''' Google News, PRNewsWire, BusinessWire '''
        pw = Google().search('"{0}" site:prnewswire.com'.format(company_name))
        bw = Google().search('"{0}" site:businesswire.com'.format(company_name))
        mw = Google().search('"{0}" site:marketwired.com'.format(company_name))
        nw = Google().search('"{0}" site:newswire.ca'.format(company_name))
        rt = Google().search('"{0}" site:reuters.com'.format(company_name))

        p = pd.concat([pw, bw, mw, nw, rt])
        p = p.drop_duplicates()
        p['date'] = [span.split('Business Wire')[-1].split('...')[0].strip() for span in p.link_span]
        p['description'] = ["".join(span.split('...')[1:]).strip() for span in p.link_span]
        p['date'] = [span.split('...')[0].strip() for span in p.link_span]
        p["timestamp"] = [Helper()._str_to_timestamp(i) for i in p.date]
        p['title'] = p['link_text']

        p = p.drop('link_text',1)
        p = p.drop('url',1)
        p = p.drop('link_span',1)
        #for i in p.timestamp: print i

        press = {'data':p.to_dict('records'), 'company_name':company_name}
        press["domain"] = domain
        #CompanyExtraInfoCrawl()._persist(press, "press", api_key)

    def _news(self, domain, api_key="", company_name=""):
        # TODO - include general info links
        browser = Browser('phantomjs')
        browser.visit('http://google.com')
        browser.find_by_name('q').first.fill(company_name)
        browser.find_by_name('btnG').first.click()
        browser.find_link_by_text('News').first.click()
        url = browser.evaluate_script("document.URL")
        url = url+"&tbs=qdr:m,sbd:1"+"&num=100&filter=0&start=0"
        browser.visit(url)
        pages = pd.DataFrame()
        df = Google()._results_html_to_df(browser.html)

        pages = pages.append(df)
        #print browser.find_by_css('td > a') 
        if browser.find_by_css('td > a') == []: 
            pages = pages.to_dict('r')
            pages = {'data':pages, 'company_name':company_name,"domain":domain}
            CompanyExtraInfoCrawl()._persist(pages, "general_news", api_key)

        try:
          _next = browser.find_by_css('td > a')[-1].text
        except:
          _next = None
        if _next:
            while "Next" in _next:
                browser.find_by_css('td > a')[-1].click()
                df = Google()._results_html_to_df(browser.html)
                pages = pages.append(df)

        #pages = pages[~pages.title.str.contains("press release")]
        pages = pages[pages.link_span.str.contains('(?i){0}'.format(company_name))]
        pages.columns = ['link','description','title','info','']
        pages['date'] = [i.split('-')[-1] for i in pages['info']]
        pages["timestamp"] = [Helper()._str__to_timestamp(i) for i in pages.date]
        pages['news_source'] = [i.split('-')[0] for i in pages['info']]
        pages = pages.drop_duplicates()
        del pages[""]
        print pages.columns

        pages = pages.to_dict('r')
        pages = {'data':pages, 'company_name':company_name,"domain":domain}
        CompanyExtraInfoCrawl()._persist(pages, "general_news", api_key)

    def _related(self, domain, api_key="", name=""):
        companies = Google().search("related:{0}".format(domain), 10)
        companies = companies.drop_duplicates()
        companies.columns = ['link','description','title','lol','lmao']
        data = {'data':companies.to_dict('r'),"domain":domain,"company_name":name}
        data["api_key"] = api_key
        CompanyExtraInfoCrawl()._persist(data, "similar", api_key)

    def _fundings(self, company_name):
        ''' Also find crunchbase handle '''
        results = Google().search("{0} site:crunchbase.com/organization".format(company_name))
        # scrape funding rounds
        return fundings

    def youtube_channel(self, company_name):
        ''' '''

    def _social_profiles(self, domain):
        fb = Google().search("{0} site:facebook.com/".format(domain))
        tw = Google().search("{0} site:twitter.com/".format(domain))
        return social_profiles

    def _technologies(self, domain, api_key="", name=""):
        ''' BuiltWith '''
        html = requests.get('https://builtwith.com/'+domain).text
        bs = BeautifulSoup(html)
        technologies = []
        for div in bs.find('div',{'class':'span8'}).find_all('div'):
            if 'titleBox' in div.get('class'):
                tech_name = div.text.split('View Global ')[0]
                continue
            tech_name = div.find('h3').text.strip()
            tech_desc = div.find('p').text if div.find('p') else ""
            logo = "http:"+div.find('img')['src']
            vals = [tech_name, tech_desc, logo, tech_name]
            names = ['tech_name', 'tech_desc', 'logo', 'tech_name']
            technologies.append(dict(zip(names, vals)))
        info = {'data':technologies, "domain":domain, "api_key":api_key}
        info["company_name"] = name
        CompanyExtraInfoCrawl()._persist(info,"builtwith",api_key)

    def _traffic_analysis(self, domain):
        ''' Compete.com, Alexa, SimilarWeb '''
        traffic = requests.get('https://similarweb.com/'+domain)
        traffic = requests.get('https://alexa.com/siteinfo/'+domain)
        return traffic

    def _employees(self, domain, api_key="", company_name="", keyword=""):
        ''' Linkedin Scrape '''
        # TODO - add linkedin directory search
        ''' Linkedin Scrape'''
        args = '-inurl:"/dir/" -inurl:"/find/" -inurl:"/updates"'
        args = args+' -inurl:"job" -inurl:"jobs2" -inurl:"company"'
        qry = '"at {0}" {1} {2} site:linkedin.com'
        qry = qry.format(company_name, args, keyword)
        results = Google().search(qry, 10)
        if results.empty: 
            if domain == "": 
                ''' return results '''
            else:
                results = Google().search(qry.format(domain, args, keyword))
        results = results.dropna()
        results = Google()._google_df_to_linkedin_df(results)
        _name = '(?i){0}'.format(company_name)
        if " " in company_name:
            results['company_score'] = [fuzz.partial_ratio(_name, company) 
                                        for company in results.company]
        else:
            results['company_score'] = [fuzz.ratio(_name, company) 
                                        for company in results.company]
        if keyword != "":
            results['score'] = [fuzz.ratio(keyword, title) 
                                for title in results.title]
            results = results[results.score > 75]

        results = results[results.company_score > 64]
        results = results.drop_duplicates()
        data = {'data': results.to_dict('r'), 'company_name':company_name}
        data["domain"] = domain
        CompanyExtraInfoCrawl()._persist(data, "employees", api_key)

        job = rq.get_current_job()
        if "queue_name" in job.meta.keys():
          if RQueue()._has_completed(job.meta["queue_name"]):
            q.enqueue(Jigsaw()._upload_csv, job.meta["company_name"])
        return results

    def _whois_info(self, domain):
        ''' Glean Info From Here'''

    def _crunchbase_profile(self, company_name):
        ''' Crunchbase Profile '''

    def _angellist_profile(self, company_name):
        ''' Angellist Profile '''

    def _research_report(self, _report):
        _report = Parse()._pointer("SignalReport", _report)
        qry={"where":json.dumps({"report": _report})}
        qry["limit"] = 1000
        # TODO - where companies are null / undefined
        signals = Prospecter().get("CompanySignal", qry).json()["results"]
        api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
        for company in signals:
            company_name = company["company_name"]
            q.enqueue(Companies()._research, company_name, api_key)

    def _score_report(self, _report):
        _report = Parse()._pointer("SignalReport", _report)
        qry={"where":json.dumps({"report": _report})}
        qry["limit"] = 1000
        # TODO - where companies are null / undefined
        signals = Prospecter().get("CompanySignal", qry).json()["results"]
        api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
        for company in signals:
            company_name = company["company_name"]
            q.enqueue(CompanyScore()._company_info, company_name)

    def _bulk_upload(self, data, user):
        print user
        data, user = json.loads(data), json.loads(user)
        _data = pd.DataFrame(data)[["company_name"]]
        _user = Parse()._pointer("_User", user["objectId"])
        _data["user"] = [_user for i in _data.index]
        _data["user_company"] = [user["user_company"] for i in _data.index]
        _data["user_company"]
        _list = {"user":_user, "user_company":user["user_company"], 
                 "list_type":"upload",
                 "name":"Upload - "+arrow.utcnow().format("DD-MM-YYYY")}
        _list = Prospecter().create("CompanyProspectList", _list).json()
        print _list
        _list = Parse()._pointer("CompanyProspectList", _list["objectId"])
        _data["lists"] =  [[_list] for i in _data.index]
        Prospecter()._batch_df_create("CompanyProspect", _data)
        for i in data: 
            #q.enqueue(Companies()._bulk, i["company_name"])
            r=requests.get("https://clear-spark.herokuapp.com/v1/companies/research",
                           params={"bulk":"bulk",
                 "api_key":"9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8",
                                   "company_name":i["company_name"]})
            print r.text

    def _bulk(self, company_name, api_key=""):
        qry = {'where':json.dumps({'company_name':company_name})}
        company = Parse().get('Company', qry).json()['results']
        company_crawl = Parse().get('CompanyInfoCrawl', qry).json()['results']
        company = False
        # check if company info crawl
        print company
        if company:
            #Webhook()._post(api_key, company[0], 'company_info')
            Webhook()._update_company_info(company[0])
            return company[0]
            '''
            elif len(company_crawl) < 5:
                api_key = "9a31a1defcdc87a618e12970435fd44741d7b88794f7396cbec486b8"
                q.enqueue(CompanyScore()._company_info, company_name, api_key)
                return {'Scoring has started.': True}
            '''
        else:
            print "STARTING RESEARCH"
            q.enqueue(Companies()._research, company_name, api_key)
            return {'Research has started.': True}

    def _research(self, name, api_key="", prospect_name=""):
        # Primary Research
        j9 = q.enqueue(Facebook()._company_profile, name, api_key,timeout=6000)
        j10 = q.enqueue(Twitter()._company_profile, name, api_key,timeout=6000)
        j11 = q.enqueue(Indeed()._company_profile, name, api_key,timeout=6000)
        j0 =q.enqueue(BusinessWeek()._company_profile, name, api_key,timeout=6000)
        j1 = q.enqueue(Zoominfo()._company_profile, name, api_key,timeout=6000)
        j2 = q.enqueue(Linkedin()._company_profile, name, api_key,timeout=6000)
        j3 = q.enqueue(YellowPages()._company_profile, name, api_key,timeout=6000)
        j4= q.enqueue(Yelp()._company_profile, name, api_key,timeout=6000)
        j5 = q.enqueue(Forbes()._company_profile, name, api_key,timeout=6000)
        j6 = q.enqueue(GlassDoor()._company_profile, name, api_key,timeout=6000)
        j7 = q.enqueue(Hoovers()._company_profile, name, api_key,timeout=6000)
        j8 = q.enqueue(Crunchbase()._company_profile, name, api_key,timeout=6000)
        jobs = [j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11]
        for job in jobs:
            RQueue()._meta(job, "{0}_{1}".format(name, api_key), prospect_name)
        # TODO - jigsaw
        # TODO - google plus

    def _domain_research(self, domain, api_key="", name="", prospect_name=""):
        # Primary Research
        if name == "": name=domain
        x = 6000
        j1 = q.enqueue(Zoominfo()._domain_search, domain, api_key, name, timeout=x)
        j2 = q.enqueue(Linkedin()._domain_search, domain, api_key,name,timeout=x)
        j3 = q.enqueue(YellowPages()._domain_search, domain, api_key,name,timeout=x)
        j4= q.enqueue(Yelp()._domain_search, domain, api_key, name,timeout=x)
        j5 = q.enqueue(Forbes()._domain_search, domain, api_key, name,timeout=x)
        j6 = q.enqueue(GlassDoor()._domain_search, domain, api_key, name,timeout=x)
        j7 = q.enqueue(Hoovers()._domain_search, domain, api_key, name,timeout=x)
        j8 = q.enqueue(Crunchbase()._domain_search, domain, api_key, name,timeout=x)
        j9 = q.enqueue(Facebook()._domain_search, domain, api_key, name,timeout=x)
        j10 = q.enqueue(Twitter()._domain_search, domain, api_key, name,timeout=x)
        j11 = q.enqueue(Indeed()._domain_search, domain, api_key, name,timeout=x)
        jobs = [j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11]
        for job in jobs:
            RQueue()._meta(job, "{0}_{1}".format(name, api_key), prospect_name)

    def _secondary_research(self, name, domain, api_key=""):
        # Secondary Research - sometimes require location or domain
        if name == "": name = domain
        x = 6000
        j0 = q.enqueue(Companies()._company_blog, domain, api_key, name, timeout=x)
        # Secondary Research - sometimes require location or domain
        if name == "": name = domain
        x = 6000
        j0 = q.enqueue(Companies()._company_blog, domain, api_key, name, timeout=x)
        j2 = q.enqueue(GlassDoor()._reviews, domain, api_key, name, timeout=x)
        j3 = q.enqueue(Companies()._press_releases,domain, api_key, name, timeout=x)
        j4 = q.enqueue(Companies()._news, domain, api_key, name, timeout=x)
        j5 = q.enqueue(Companies()._hiring, domain, api_key, name, timeout=x)
        j6 = q.enqueue(Twitter()._daily_news, domain, api_key, name, timeout=x)
        j7 = q.enqueue(Facebook()._daily_news, domain, api_key, name, timeout=x)
        j8 = q.enqueue(Linkedin()._daily_news, domain, api_key, name, timeout=x)

        # TODO - general pages on their site
        jobs = [j0,j2,j3,j4,j5,j6,j7,j8]
        for job in jobs: RQueue()._meta(job, "{0}_{1}".format(name, api_key))

        #TODO - mixrank ads research
        #q.enqueue(Crunchbase()._fundings, domain)
        #q.enqueue(Companies()._traffic_analysis, domain)
        #q.enqueue(Companies()._whois_info, domain)
    #def _daily_secondary_reseach

    def _daily_secondary_research(self, name, domain, api_key=""):
        # Secondary Research - sometimes require location or domain
        if name == "": name = domain
        x = 6000
        j0 = q.enqueue(Companies()._company_blog, domain, api_key, name, timeout=x)
        # Secondary Research - sometimes require location or domain
        if name == "": name = domain
        x = 6000
        j0 = q.enqueue(Companies()._company_blog, domain, api_key, name, timeout=x)
        j2 = q.enqueue(GlassDoor()._reviews, domain, api_key, name, timeout=x)
        j3 = q.enqueue(Companies()._press_releases,domain, api_key, name, timeout=x)
        j4 = q.enqueue(Companies()._news, domain, api_key, name, timeout=x)
        j5 = q.enqueue(Companies()._hiring, domain, api_key, name, timeout=x)
        j6 = q.enqueue(Twitter()._daily_news, domain, api_key, name, timeout=x)
        j7 = q.enqueue(Facebook()._daily_news, domain, api_key, name, timeout=x)
        j8 = q.enqueue(Linkedin()._daily_news, domain, api_key, name, timeout=x)

        # TODO - general pages on their site
        jobs = [j0,j2,j3,j4,j5,j6,j7,j8]
        for job in jobs: RQueue()._meta(job, "{0}_{1}".format(name, api_key))

    ''' In Use Methods '''
    def _email_pattern(self, domain):
        ''' ClearSpark '''
        patterns = EmailGuess().search_sources(domain)
        return patterns

    def _get_info(self, company_name, api_key=""):
        profile = Linkedin()._company_profile(company_name, api_key)
        if type(profile) is str: 
            profile = Zoominfo().search(company_name, api_key)
            logger.info("zoominfo company "+str(profile))
        #result = Parse()._add_company(profile, company_name)
        return profile

    def _get_long_info(self, company_name):
        profile = Linkedin()._company_profile(company_name)
        _profile = Zoominfo().search(company_name)
        print profile
        if type(profile) is str: 
            profile = Zoominfo().search(company_name)
            logger.info("zoominfo company "+str(profile))
        if type(profile) is not str and type(_profile) is not str: 
            profile['phone'] = _profile['phone']
        result = Parse()._add_company(profile, company_name)
        return profile

    def _async_get_info(self, company_name, update_object=False):
        profile = self._get_info(company_name)
        Parse().update('Prospect/'+str(update_object), profile, True).json()
        q.enqueue(EmailGuess().start_search, profile['domain'])

    def _get_info_webhook(self, company_name, objectId):
        profile = self._get_long_info(company_name)
        res = Parse().update('Prospect/'+objectId, profile, True).json()
        result = Parse().update('CompanyProspect/'+objectId, profile, True)
        return profile

class CompanyTrends:
    def linkedin_followers():
        ''' '''

    def alexa_rank():
        ''' '''

    def twitter_followers():
        ''' '''

    def angellist_followers():
        ''' '''

    def google_search_volume():
        ''' '''
