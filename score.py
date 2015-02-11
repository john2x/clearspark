import json
import pandas as pd
import requests
from webhook import Webhook
from fullcontact import FullContact
from email_guess import EmailGuess
from queue import RQueue
from parse import Parse
from sources import Sources
# Scoring 
from rq import Queue
from worker import conn
q = Queue(connection=conn)


class Score:
    def _email_pattern(self, domain, api_key=""):
        print ''' Score email pattern based on number of occurrences '''
        qry = {'where':json.dumps({'domain': domain})}
        crawls = Parse().get('CompanyEmailPatternCrawl', qry)
        crawls = pd.DataFrame(crawls.json()['results'])

        df = crawls[crawls.pattern.notnull()].drop_duplicates('email')
        _df = df[df.crawl_source != "mx_check"]
        df = df[df.crawl_source == "mx_check"].drop_duplicates('pattern')
        if len(df.pattern) > 2: df = df[df.crawl_source != "mx_check"]
        df = _df.append(df)
        df = df.pattern.value_counts()

        score = pd.DataFrame()
        score['pattern'], score['freq'] = df.index, df.values
        score['score'] = [freq / float(score.freq.sum()) for freq in score['freq']]
        score['source'], score['tried'] = 'clearspark', False
        score = score.fillna("")
        score = score.to_dict('records')
        print score, api_key
        score = {'domain':domain, 'company_email_pattern':score}
        self._find_if_object_exists('EmailPattern','domain', domain, score)

        # TODO - add date crawled to score
        if RQueue()._has_completed("{0}_{1}".format(domain, api_key)):
            if score['company_email_pattern'] == []:
                score['email_guess'] = EmailGuess()._random()
                q.enqueue(Sources()._jigsaw_search, domain)
            Webhook()._update_company_email_pattern(score)

    def _remove_non_ascii(self, text):
        ''.join(i for i in text if ord(i)<128)

    def _prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''

    def _company_prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''

    def _find_if_object_exists(self, class_name, column, value, data):
        qry = json.dumps({column: value})
        obj = Parse().get(class_name, {'where': qry}).json()['results']
        print obj
        if obj: 
            print "NEW UPDATE OLD", class_name+'/'+obj[0]['objectId']
            print Parse().update(class_name+'/'+obj[0]['objectId'], data).json()
        else: 
            print "NEW CREATE NEW"
            print Parse().create(class_name, data).json()

    def _score(self, patterns):
        ''' Old Scoring To Multiple Patterns '''
        print "_score"
        if patterns.shape[0] == 0:
            return patterns
        total = len(patterns.drop_duplicates().pattern)
        values = patterns.drop_duplicates('name').pattern.value_counts()
        upload = patterns.drop_duplicates('name')
        upload['instances'] = [i for i in patterns.email.value_counts()]
        upload['score'] = [int(float(i)/total*100) for i in values]
        return upload

