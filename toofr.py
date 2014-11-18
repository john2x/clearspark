import requests
import tldextract 

def make(name, website):
  fn = name.split(' ')[0]
  ln = name.split(' ')[-1]
  key = '89be5b202d383a74791677a735e7ac48'
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


