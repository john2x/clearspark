import requests
import json

class Parse:
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
