import requests
import json
import math
import time
import pandas as pd

class Parse:
    def __init__(self):
        self._url = 'https://api.parse.com/1/classes/{0}'
        self._batch_url = 'https://api.parse.com/1/batch'
        self._headers = {
          'Content-Type': 'application/json',
          'X-Parse-Application-Id': 'aBbOmPBkUou54VeIrtHhIU7a6PQSKEkXLCjm6cJf',
          'X-Parse-REST-API-Key': 'VIC4qwcpgDtKp5HgDcC58b8mwqg0yS8wwrubTtUN'}
        self._master_headers = {
          "X-Parse-Application-Id": "aBbOmPBkUou54VeIrtHhIU7a6PQSKEkXLCjm6cJf",
          "X-Parse-Master-Key": "2gWbC8qMhDwQyBLVoaIz1w7M5zI3zPgawDYE7oXL",
          "Content-Type": "application/json"
        }

    def _pointer(self, className, objectId):
        return {
            '__type' : 'Pointer',
            'className':className,
            'objectId':objectId
        }

    def create(self, className, data):
        r = requests.post(self._url.format(className),
                          data=json.dumps(data),
                          headers=self._headers)
        return r

    def get(self, className, qry={}):
          r = requests.get(self._url.format(className),
                           params=qry,
                           headers=self._master_headers)
          return r

    def _batch_df_create(self, className, df):
        print "BATCH DF STARTED"
        responses = []
        df = df.reset_index().drop('index', 1)
        for i in range(int(math.ceil(df.shape[0]/50.0))):
            #TODO - fill nan with something
            #TODO - remove keyword columns
            error = True
            while error:
                a, b, data = i*50, (i+1)*50-1, pd.DataFrame()
                tmp = df.ix[a:b]
                data['body']    = tmp.to_dict('r')
                data['method']  = ["POST" for i in tmp.index]
                data['path']    = ["/1/classes/"+className for i in tmp.index]
                data            = {"requests" : data.to_dict('r')}
                r = requests.post(self._batch_url,
                                  data=json.dumps(data),
                                  headers=self._headers)
                error = type(r.json()) is dict
                print "IS ERROR??? --> ", r.json(), error
                if error:
                    time.sleep(10)
                    print "batch df error occurred. retrying"
            responses = responses+r.json()
        return responses

    def update(self, className, data):
        r = requests.put(self._url.format(className),
                         data=json.dumps(data),
                         headers=self._master_headers)
        return r

    ''' ClearSpark Specific Methods '''
    def _add_company(self, company, company_qry):
        ''' '''
        print "ADD COMPANY"
        r = self.create('Company', company).json()
        print r
        if 'error' in r.keys() and type(company) is not str:
            print company['domain']
            qry = json.dumps({"domain": company['domain']})
            r = self.get('Company', {'where': qry}).json()['results'][0]
            r = self.update('Company/'+r['objectId'], 
                        {"search_queries":
                        {"__op":"AddUnique","objects":[company_qry]}}).json()
            print r
            
class Prospecter:
  def __init__(self):
    self._url = 'https://api.parse.com/1/classes/{0}'
    self._batch_url = 'https://api.parse.com/1/batch'
    self._headers = {
      "X-Parse-Application-Id": "N85QOkteEEQkuZVJKAvt8MVes0sjG6qNpEGqQFVJ",
      "X-Parse-REST-API-Key": "VN6EwVyBZwO1uphsBPsau8t7JQRp00UM3KYsiiQb",
      "Content-Type": "application/json"
    }
    self._master_headers = {
      "X-Parse-Application-Id": "N85QOkteEEQkuZVJKAvt8MVes0sjG6qNpEGqQFVJ",
      "X-Parse-Master-Key": "RQgSIyw9rxC4xn4KlsIEYzDIpkxNIUlLz70akJcT",
      "Content-Type": "application/json"
    }

  def get(self, className, qry={}):
    r = requests.get(self._url.format(className),
                     params=qry,
                     headers=self._headers)
    return r

  def _batch_df_create(self, className, df):
      print "BATCH DF STARTED"
      responses = []
      df = df.reset_index().drop('index', 1)
      for i in range(int(math.ceil(df.shape[0]/50.0))):
          #TODO - fill nan with something
          #TODO - remove keyword columns
          error = True
          while error:
              a, b, data = i*50, (i+1)*50-1, pd.DataFrame()
              tmp = df.ix[a:b]
              data['body']    = tmp.to_dict('r')
              data['method']  = ["POST" for i in tmp.index]
              data['path']    = ["/1/classes/"+className for i in tmp.index]
              data            = {"requests" : data.to_dict('r')}
              r = requests.post(self._batch_url,
                                data=json.dumps(data),
                                headers=self._headers)
              error = type(r.json()) is dict
              print "IS ERROR??? --> ", r.json(), error
              if error:
                  time.sleep(10)
                  print "batch df error occurred. retrying"
          responses = responses+r.json()
      return responses

  def _pointer(self, className, objectId):
      return {
          '__type' : 'Pointer',
          'className':className,
          'objectId':objectId
      }

  def create(self, className, data):
      r = requests.post(self._url.format(className),
                        data=json.dumps(data),
                        headers=self._headers)
      return r

  def update(self, className, data):
      r = requests.put(self._url.format(className),
                       data=json.dumps(data),
                       headers=self._master_headers)
      return r
