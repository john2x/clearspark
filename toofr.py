import requests
import tldextract 

class Toofr():
    def make(name, website):
      fn = name.split(' ')[0]
      ln = name.split(' ')[-1]
      key = '2824d8710563abbb276514745b2ec619'
      domain = "{}.{}".format(tldextract.extract(website).domain, tldextract.extract(website).tld)
      toofr_data = {'first': fn,
              'last' : ln,
              'key'  : key,
              'domain':domain}
      r = requests.get('http://toofr.com/api/make',params=toofr_data)
      #email = r.json()['response']['email'] if 'email' in r.json()['response'].keys() else "no_email"

      if 'email' in r.json()['response'].keys():
        return r.json()['response']['email']  
      else:
        return "no_email"

    def get(self, domain):
      key = '2824d8710563abbb276514745b2ec619'
      toofr_data = {
        "key":key,
        "domain":domain,
      }
      r = requests.get('http://toofr.com/api/get', params=toofr_data)
      if r.json()["response"]["description"]:
          # match pattern with facebook

