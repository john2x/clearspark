import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib

class Google:
  def linkedin_search(self, qry, pages=1):
      qry = qry + ' site:linkedin.com/in/ OR site:linkedin.com/pub/'
      qry = qry + '-site:linkedin.com/pub/dir/'
      res = pd.DataFrame()
      for page in range(pages):
          print page
          args = urllib.urlencode({'q':qry,'start':page*100,'num':100})
          url = 'https://www.google.com/search?'+ args
          cloak = "https://crawlera.p.mashape.com/fetch"
          headers = {"X-Mashape-Key": 
                     "pdL7tBtCRXmshjM0GeRxnbyhpWzNp13kguyjsnxPTjSv8foPKA"}
          r = requests.get(cloak, params={'url':url}, headers=headers)
          res.append(self._results_to_html(r.text))
      return res

  def search(self, qry, pages=1):
      res = pd.DataFrame()
      for page in range(pages):
          print page
          args = urllib.urlencode({'q':qry,'start':page*100,'num':100})
          url = 'https://www.google.com/search?'+ args
          cloak = "https://crawlera.p.mashape.com/fetch"
          headers = {"X-Mashape-Key": 
                     "pdL7tBtCRXmshjM0GeRxnbyhpWzNp13kguyjsnxPTjSv8foPKA"}
          r = requests.get(cloak, params={'url':url}, headers=headers)
          res = res.append(self._results_html_to_df(r.text))
      return res

  def _results_html_to_df(self, search_result_html):
      leads = pd.DataFrame()
      listings = BeautifulSoup(search_result_html).findAll('li',{'class':'g'})
      for lead in listings:
          link_text = lead.find('h3').text
          link = lead.find('a')['href'].split('=')[1].split('&')[0]
          url = lead.find('cite').text
          link_span = lead.find('span',{'class':'st'}).text
          try:
              title = lead.find('div',{'class':'slp'}).text
          except:
              title = ""
          leads = leads.append(dict(zip(['link_text','url','title','link_span','link'],
                                        [link_text, url,title,link_span,link])),
                                        ignore_index=True)
      return leads
