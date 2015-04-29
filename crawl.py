from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
import tldextract
from parse import *
import pandas as pd

class CompanyInfoCrawl:
    def _persist(self, data, source="", api_key=""):
        data['source'] = source
        data['api_key'] = api_key
        print source
        crawl = Parse().create('CompanyInfoCrawl', data).json()
        # if error
        print crawl
        return crawl

    def _score(self, crawl_id):
        crawl = Parse()._pointer('CompanyInfoCrawl', crawl['objectId'])
        company = Parse().get('Company', {'where':json.dumps({'domain':data['domain']})}).json()
        data['crawl_source'] = [source for i in range(data.shape[0])]
        print company
        if company['results']:
            company = 'Company/'+company['results'][0]['objectId'], 
            data = {'__op':'AddUnique', "objects":[crawl]}
            print "update", Parse().update(company, {'crawls': data}).json()
        else:
            print Parse().create(company, {'crawls':[crawl],'domain':data['domain']}).json()

class CompanyEmailPatternCrawl:
    def _bulk_persist(self, data):
        ''' '''

    def _persist(self, data, source="", api_key=""):
        print source, data, "PERSISTING YOYO"
        data['crawl_source'] = source
        data['api_key'] = api_key
        for index, row in data.iterrows():
            print row.to_dict()
            r  = Parse().create('CompanyEmailPatternCrawl', row.to_dict()).json()
            print r

class CompanyExtraInfoCrawl:
    def _persist(self, data, source, api_key=""):
        if source == "blog_data": _source = "CompanyBlogPost"
        elif source == "builtwith": _source = "CompanyTechnology"
        elif source ==  "press": _source = "CompanyPressRelease"
        elif source ==  "glassdoor_reviews": _source = "CompanyGlassdoorReview"
        elif source ==  "employees": _source = "CompanyEmployee"
        elif source ==  "similar": _source = "CompanySimilar"
        elif source ==  "hiring": _source = "CompanyHiring"
        elif source ==  "general_news": _source = "CompanyNews"
        elif source ==  "linkedin_posts": _source = "CompanyLinkedinPost"
        elif source ==  "facebook_posts": _source = "CompanyFacebookPost"
        elif source ==  "tweets": _source = "CompanyTweet"
        #TODO - News
        #TODO - CompanySocialMedia - Linkedin Posts, Tweets, Facebook Posts
        #TODO - prevent duplicates
        #TODO - batch create data

        print _source
        #print data.keys()
        _data = pd.DataFrame(data['data'])
        if "company_name" in data.keys():
          _data["company_name"] = data["company_name"]
        _data["api_key"] = api_key
        if "domain" in data.keys(): _data["domain"] = data["domain"]
        res1=Prospecter()._batch_df_create(_source, _data)
        res2=Parse()._batch_df_create(_source, _data)
        print res1, res2
        data["source"],data["_source"]=source, _source
        timestamps = _data.timestamp.tolist()
        #TODO - make sure only success events are recorded
        _data = [{"event":[Parse()._pointer(_source, i["success"]["objectId"])],
                  "company_name":data["company_name"],
                  "domain":data["domain"]} for i in res1]
        _data = pd.DataFrame(_data)
        _data["timestamp"] = [int(i) for i in timestamps]

        print Prospecter()._batch_df_create("CompanyEvent", _data)
        print Parse()._batch_df_create("CompanyEvent", _data)


