# Scoring 

class Score:
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

    def _email_pattern(self, data):
        ''' Score email pattern based on number of occurrences '''
        domain = data["domain"]
        crawls = Parse().get('CompanyEmailPatternCrawl', {'where':{'domain':domain}})
        crawls = crawl.json()['results']
        crawls = pd.concat([pd.DataFrame(_crawl) for _crawl in crawl['results']])
        # get value_counts for pattern column
        # add percentage confidence
        qry = {'where':{'domain':domain}}
        email_pattern = Parse().get('CompanyEmailPattern', qry).json()
        if email_pattern:
            pattern = 'CompanyEmailPattern/'+company['results']['objectId']
            Parse().update(pattern, crawls.to_dict('records'))
        else:
            Parse().create('CompanyEmailPattern', crawls.to_dict('records'))

    def _company_infomation(self, data):
        ''' Replace blank strings with company info '''
        domain = data["domain"]
        crawl = Parse().get('CompanyInfoCrawl', {'where':{'domain':domain}}).json()
        crawls = pd.concat([pd.DataFrame(_crawl) for _crawl in crawl['results']])
        crawls = self._source_score(crawls)
        # remove duplicate domain keep the one with the highest score
        company = Parse().get('Company', {'where':{'domain':domain}}).json()
        if company:
            Parse().update('Company/'+company['results']['objectId'],
                          crawls.to_dict('records'))
        else:
            Parse().create('Company', crawls.to_dict('records'))

    def _source_score(self, df):
        df.ix[df.source == "linkedin", 'score']    = 10
        df.ix[df.source == "zoominfo", 'score']    = 10
        df.ix[df.source == "yelp", 'score']        = 5
        df.ix[df.source == "yellowpages", 'score'] = 5
        df.ix[df.source == "facebook", 'score']    = 1
        df.ix[df.source == "twitter", 'score']     = 1
        return df

    def _prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''

    def _company_prospect_score(self, data):
        ''' How hot a prospect they are based on a couple of factors '''


