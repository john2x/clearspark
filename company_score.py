import json
import pandas as pd
import requests
from webhook import Webhook
from fullcontact import FullContact
from email_guess import EmailGuess
from queue import RQueue
from parse import Parse
import companies
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from companies import Companies
from queue import RQueue

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class CompanyScore:
    def _bulk_info():
      ''' add industry, phones, and suff'''

    def _company_info(self, company_name, api_key=""):
        #TODO - company_name = self._remove_non_ascii(company_name) add to save
        qry = {'where':json.dumps({'company_name': company_name}), 'limit':1000}
        qry['order'] = '-createdAt'
        crawls = Parse().get('CompanyInfoCrawl', qry).json()['results']

        if not crawls: 
            # start crawls
            return company_name
        crawls = self._source_score(pd.DataFrame(crawls))
        #crawls = crawls[crawls.api_key == api_key]
        crawls['name_score'] = [fuzz.token_sort_ratio(row['name'], row.company_name) 
                                for index, row in crawls.iterrows()]
        crawls = crawls[crawls.name_score > 70].append(crawls[crawls.name.isnull()])
        #crawls = crawls[["press", 'source_score', 'source', 'createdAt', 'domain']]
        final = {}
        #print crawls.press.dropna()
        for col in crawls.columns:
            if col in ['source_score', 'source', 'createdAt']: continue
            df = crawls[[col, 'source_score', 'source', 'createdAt']]
            if df[col].dropna().empty: continue
            if type(list(df[col].dropna())[0]) == list: 
                df[col] = df[col].dropna().apply(tuple)
            try: df = df[df[col] != ""]
            except: "lol"
            try:
                df = df[df[col].notnull()]
                df = [source[1].sort('createdAt').drop_duplicates(col, True)
                          for source in df.groupby(col)]
                df = [_df for _df in df if _df is not None]
                df = [pd.DataFrame(columns=['source_score',col])] if len(df) is 0 else df
                df = pd.concat(df).sort('source_score')[col]
                if list(df): final[col] = list(df)[-1]
            except: "lol"


        if 'industry' in final.keys(): 
          try:
              final['industry'] = final['industry'][0]
          except:
              final["industry"] = ""

        try:
          final['industry_keywords'] = list(set(crawls.industry.dropna().sum()))
        except:
          final['industry_keywords'] = []

        if 'address' in final.keys():
            final['address'] = FullContact()._normalize_location(final['address'])
        try:
            final['handles'] = crawls[['source','handle']].dropna()
            final['handles'] = final['handles'].drop_duplicates().to_dict('r')
        except:
            "lol"
          
        try:
            final['phones'] = crawls[['source','phone']].dropna()
            final['phones'] = final['phones'].drop_duplicates().to_dict('r')
        except:
            "lmao"
        # TODO - if company_name exists update
        # TODO - find if domain exists under different company_name then update 
        final = self._prettify_fields(final)
        del final["name_score"]
        print json.dumps(final)
        self._add_to_clearspark_db('Company', 'company_name', company_name, final)

        # TODO - find main domain from domain -> ie canon.ca should be canon.com
        # clean data - ie titleify fields, and lowercase domain
        # TODO - start a domain search with the deduced domain and the company_name
        print "RQUEUE CHECK"
        if "domain" in final.keys():
            domain = final["domain"]

        '''
        if len(RQueue()._results("{0}_{1}".format(company_name, api_key))) == 1:
            q.enqueue(Companies()._domain_research, domain, api_key, company_name)
            q.enqueue(Companies()._secondary_research, company_name, domain, api_key)
        '''

        if RQueue()._has_completed("{0}_{1}".format(company_name, api_key)):
            #q.enqueue(Companies()._domain_research, domain, api_key, company_name)
            q.enqueue(Companies()._secondary_research, company_name, domain, api_key)

            print "WEBHOOK <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            if "company_name" in final.keys():
                Webhook()._update_company_info(final)
            '''
            job = q.enqueue(EmailGuess().search_sources, final["domain"],api_key,"")
            job.meta["{0}_{1}".format(company_name, api_key)] = True
            job.save()
            '''
            for domain in crawls.domain.dropna().drop_duplicates():
                job = q.enqueue(EmailGuess().search_sources, domain, api_key, "")
                RQueue()._meta("{0}_{1}".format(company_name, api_key))
        return final

    def _prettify_fields(self, final):
        if "domain" in final.keys():
          final['domain'] = final['domain'].lower()
        # titlify everything else ?
        return final

    def _add_to_clearspark_db(self, class_name, column, value, data):
        qry = json.dumps({column: value})
        obj = Parse().get(class_name, {'where': qry}).json()['results']
        #print obj
        if obj: 
            print "NEW UPDATE OLD", class_name+'/'+obj[0]['objectId']
            print Parse().update(class_name+'/'+obj[0]['objectId'], data).json()
        else: 
            print "NEW CREATE NEW"
            print Parse().create(class_name, data).json()

    def _company_check(self, company_name, domain, data, class_name="Company"):
        qry = json.dumps({'domain': domain})
        domain_check = Parse().get(class_name, {'where': qry}).json()['results']
        qry = json.dumps({'company_name': company_name})
        name_check = Parse().get(class_name, {'where': qry}).json()['results']
        if domain_check == [] and name_check == []: 
            print "NEW CREATE NEW"
            print Parse().create(class_name, data).json()
        else: 
            print "NEW UPDATE OLD", class_name+'/'+domain_check[0]['objectId']
            print Parse().update(class_name+'/'+domain_check[0]['objectId'], data).json()
            # TODO - add search query

    def _email_webhook_should_be_called(self, crawls):
        return True

    def _webhook_should_be_called(self, crawls):
        print crawls.source.drop_duplicates().shape[0]
        return True

    def _source_score(self, df):
        df.ix[df.source == "linkedin", 'source_score']     = 10
        df.ix[df.source == "zoominfo", 'source_score']     = 9
        df.ix[df.source == "yelp", 'source_score']         = 2
        df.ix[df.source == "yellowpages", 'source_score']  = 3
        df.ix[df.source == "facebook", 'source_score']     = 1
        df.ix[df.source == "twitter", 'source_score']      = 0
        df.ix[df.source == "businessweek", 'source_score'] = 4
        df.ix[df.source == "forbes", 'source_score']       = 5
        df.ix[df.source == "hoovers", 'source_score']      = 6
        df.ix[df.source == "crunchbase", 'source_score']   = 7
        df.ix[df.source == "glassdoor", 'source_score']    = 8
        return df
