import requests
import json

class Parse:
    def __init__(self):
        self._url = 'https://api.parse.com/1/classes/{0}'
        self._headers = {
          "X-Parse-Application-Id": "aBbOmPBkUou54VeIrtHhIU7a6PQSKEkXLCjm6cJf",
          "X-Parse-REST-API-Key": "VIC4qwcpgDtKp5HgDcC58b8mwqg0yS8wwrubTtUN",
          "Content-Type": "application/json"
        }
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
                         headers=self._headers)
        return r

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
