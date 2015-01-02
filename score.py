from parse import Parse
import json
import pandas as pd
import requests
from webhook import Webhook
from fullcontact import FullContact
# Scoring 

class Score:
    def _email_pattern(self, domain, api_key=""):
        print ''' Score email pattern based on number of occurrences '''
        qry = {'where':json.dumps({'domain': domain})}
        crawls = Parse().get('CompanyEmailPatternCrawl', qry)
        crawls = pd.DataFrame(crawls.json()['results'])
        df = crawls[crawls.pattern.notnull()].drop_duplicates('email')
        df = df.pattern.value_counts()

        score = pd.DataFrame()
        score['pattern'], score['freq'] = df.index, df.values
        score['score'] = [freq / float(score.freq.sum()) for freq in score['freq']]
        score = score.to_dict('records')
        print score
        self._find_if_object_exists('CompanyEmailPattern','domain', domain, score)
        # TODO - add date crawled
        # TODO - webhook should be called when all calls are complete
        if self._webhook_should_be_called(): Webhook()._post(api_key, final)

    def _company_info(self, company_name, api_key=""):
        print 'remove duplicate info keep the one with the highest score'
        qry = {'where':json.dumps({'company_name': company_name})}
        crawls = Parse().get('CompanyInfoCrawl', qry).json()
        crawls = self._source_score(pd.DataFrame(crawls['results']))
        crawls = crawls[crawls.api_key == api_key]
        final = {}
        for col in crawls.columns:
            if col == 'score': continue
            df = crawls[[col, 'score', 'source', 'createdAt']]
            df = df[(df[col].notnull()) & (df[col] != "")]
            df = [source[1].sort('createdAt').drop_duplicates(col, True) 
                  for source in _df.groupby(col)]
            df = pd.concat(df)
            df = df.sort('score')[col]
            final[col] = list(df)[-1]
        final['industry'] = final['industry'][0]
        final['address'] = FullContact()._normalize_location(final['address'])
        del final['source']
        self._find_if_object_exists('Company', 'company_name', company_name, final)
        if self._webhook_should_be_called(crawls): Webhook()._post(api_key, final)

    def _webhook_should_be_called(self, crawls):
        return crawls.source.drop_duplicates().shape[0] == 9:

    def _find_if_object_exists(self, class_name, column, value, data):
        qry = json.dumps({column: value})
        obj = Parse().get(class_name, {'where': qry}).json()['results']
        print obj
        if obj: print Parse().update(class_name+'/'+obj[0]['objectId'], data).json()
        else: print Parse().create(class_name, data).json()

    def _source_score(self, df):
        df.ix[df.source == "linkedin", 'score']    = 10
        df.ix[df.source == "zoominfo", 'score']    = 9
        df.ix[df.source == "yelp", 'score']        = 2
        df.ix[df.source == "yellowpages", 'score'] = 3
        df.ix[df.source == "facebook", 'score']    = 1
        df.ix[df.source == "twitter", 'score']     = 0
        df.ix[df.source == "businessweek", 'score']    = 4
        df.ix[df.source == "forbes", 'score']     = 5
        df.ix[df.source == "hoovers", 'score']     = 6
        df.ix[df.source == "crunchbase", 'score']     = 7
        df.ix[df.source == "glassdoor", 'score']     = 8
        return df

    def _prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''

    def _company_prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''

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

