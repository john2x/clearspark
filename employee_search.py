from splinter import Browser
import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib
import time
from google import Google
import rq
from queue import RQueue
from rq import Queue
from worker import conn
from crawl import *
from fuzzywuzzy import fuzz

q = Queue(connection=conn)

class LinkedinTitleDir:
  def _search(self, company_name, api_key=""):
    qry = 'site:linkedin.com inurl:"at-{0}" inurl:title -inurl:job'
    #TODO - remove, all [".","'",","]
    name = company_name.strip().lower().replace(" ","-")
    dirs = Google().search(qry.format(name), 10)
    for url in dirs.url:
      q.enqueue(LinkedinTitleDir().parse, url, company_name)

  def parse(self, url, company_name):
    cache = Google().cache(url)
    soup = BeautifulSoup(cache)
    p = []

    for i in soup.find_all("div",{"class":"entityblock"}):
        try:
          img = i.find("img")["data-delayed-url"]
        except:
          img = i.find("img")["src"]
        profile = i.find("a")["href"]
        name = i.find("h3",{"class":"name"})
        name = name.text if name else ""
        title = i.find("p",{"class":"headline"})
        title = title.text if title else ""
        city = i.find("dd")
        city = city.text if city else ""
        cols = ["img","profile","name","title","city"]
        vals = [img, profile, name, title, city]
        print vals
        p.append(dict(zip(cols, vals)))
    print p
    results = p
    if " " in company_name:
        results['company_score'] = [fuzz.partial_ratio(company_name, 
                                                       company.split("at "[-1]))
                                    for company in results.full_title]
    else:
        results['company_score'] = [fuzz.ratio(company_name, 
                                               company.split("at "[-1]))
                                    for company in results.full_title]
    results = results[(results.company_score > 64)]
    data = {'data': results.to_dict("r"), 'company_name':company_name}
    CompanyExtraInfoCrawl()._persist(data, "employees", "")

    job = rq.get_current_job()
    if "queue_name" in job.meta.keys():
      if RQueue()._has_completed(job.meta["queue_name"]):
        q.enqueue(Jigsaw()._upload_csv, job.meta["company_name"])

    return p

class GoogleSearch:
    def _employees(self, company_name="", keyword=""):
        ''' Linkedin Scrape '''
        # TODO - add linkedin directory search
        ''' Linkedin Scrape'''
        args = '-inurl:"/dir/" -inurl:"/find/" -inurl:"/updates"'
        args = args+' -inurl:"job" -inurl:"jobs2" -inurl:"company"'
        qry = '"at {0}" {1} {2} site:linkedin.com'
        qry = qry.format(company_name, args, keyword)
        results = Google().search(qry, 10)
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
        CompanyExtraInfoCrawl()._persist(data, "employees", "")

        job = rq.get_current_job()
        if "queue_name" in job.meta.keys():
          if RQueue()._has_completed(job.meta["queue_name"]):
            q.enqueue(Jigsaw()._upload_csv, job.meta["company_name"])
        return results
