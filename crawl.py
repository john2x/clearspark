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
        if source == "blog_data": source = "CompanyBlogPost"
        elif source == "builtwith": source = "CompanyTechnology"
        elif source ==  "press": source = "CompanyPressRelease"
        elif source ==  "glassdoor_reviews": source = "CompanyGlassdoorReview"
        elif source ==  "employees": source = "CompanyEmployee"
        elif source ==  "similar": source = "CompanySimilar"
        elif source ==  "hiring": source = "CompanyHiring"
        elif source ==  "general_news": source = "CompanyNews"
        elif source ==  "linkedin_posts": source = "CompanyLinkedinPost"
        elif source ==  "facebook_posts": source = "CompanyFacebookPost"
        elif source ==  "tweets": source = "CompanyTweet"
        #TODO news
        #TODO - CompanySocialMedia - Linkedin Posts, Tweets, Facebook Posts
        #TODO - prevent duplicates
        #TODO - batch create data
        print source
        _data = pd.DataFrame(data['data'])
        if "company_name" in data.keys():
          _data["company_name"] = data["company_name"]
        _data["api_key"] = api_key
        if "domain" in data.keys():
          _data["domain"] = data["domain"]
        print Prospecter()._batch_df_create(source, _data)
        print Parse()._batch_df_create(source, _data)
