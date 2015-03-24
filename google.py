import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib
from requests.auth import HTTPProxyAuth

class Crawlera:
    def get(self, url):
        cloak = "https://crawlera.p.mashape.com/fetch"
        headers = {"X-Mashape-Key": "pdL7tBtCRXmshjM0GeRxnbyhpWzNp13kguyjsnxPTjSv8foPKA"}
        r = requests.get(cloak, params={'url':url}, headers=headers)
        return r

    def _get(self, url):
        #url = "https://google.com"
        auth = HTTPProxyAuth("customero", "iUyET3ErxR")
        proxies = {"http": "paygo.crawlera.com:8010"}
        r = requests.get(url,
                         #headers=headers,
                         proxies=proxies,
                         #timeout=timeout,
                         auth=auth)
        return r

class Google:
    def linkedin_search(self, qry, pages=1):
        qry = qry + ' site:linkedin.com/in/ OR site:linkedin.com/pub/'
        qry = qry + '-site:linkedin.com/pub/dir/'
        res = pd.DataFrame()
        for page in range(pages):
            print page
            args = urllib.urlencode({'q':qry,'start':page*100,'num':100})
            url = 'https://www.google.com/search?'+ args
            '''
            cloak = "https://crawlera.p.mashape.com/fetch"
            headers = {"X-Mashape-Key":
                "pdL7tBtCRXmshjM0GeRxnbyhpWzNp13kguyjsnxPTjSv8foPKA"}
            r = requests.get(cloak, params={'url':url}, headers=headers)
            '''
            r = requests.get(url)
            res = res.append(self._results_to_html_df(r.text))
            # filter only linkedin_url
        return res

    def ec2_cache(self, url):
        url = url.replace('&', '%26')
        url='http://webcache.googleusercontent.com/search?q=cache:'+url
        return requests.get(url).text

    def ec2_search(self, qry, pages=1):
        res = pd.DataFrame()
        for page in range(pages):
            qry = self._remove_non_ascii(qry)
            args = urllib.urlencode({'q':qry,'start':page*100,'num':100})
            url = 'https://www.google.com/search?'+ args
            print url
            r = requests.get(url)
            res = res.append(self._results_html_to_df(r.text))
        return res

    def news_search(self, qry, pages=1):
        res = pd.DataFrame()
        for page in range(pages):
            print page
            args = urllib.urlencode({'q':qry,'start':page*100,'num':100})
            url = 'https://news.google.com/'+ args
            '''
            cloak = "https://crawlera.p.mashape.com/fetch"
            headers = {"X-Mashape-Key": 
                       "pdL7tBtCRXmshjM0GeRxnbyhpWzNp13kguyjsnxPTjSv8foPKA"}
            r = requests.get(cloak, params={'url':url}, headers=headers)
            '''
            r = requests.get(url)
            res = res.append(self._results_html_to_df(r.text))
        return res

    def _results_to_linkedin_df(self, html):
        ''' '''
    
    def _remove_non_ascii(self, text):
        return ''.join(i for i in text if ord(i)<128)

    def cache(self, url):
        url = url.replace('&', '%26')
        url = 'http://webcache.googleusercontent.com/search?q=cache:'+url
        cloak = "https://crawlera.p.mashape.com/fetch"
        headers = {"X-Mashape-Key": "mEol4XmA3QmshtYIjvaaqvts9kyOp1DwvVvjsnoN02b6eKv98h"}
        r = requests.get(cloak, params={'url':url}, headers=headers)
        #r = requests.get(url)
        return r.text

    def search(self, qry, pages=1, period=""):
        res = pd.DataFrame()
        for page in range(pages):
            print page
            qry = self._remove_non_ascii(qry)
            args = {'q':qry,'start':page*100,'num':100,"filter":0}
            if period != "":
              args["tbs"] = "qdr:{0},sbd:1".format(period)
            args = urllib.urlencode(args)
            url = 'https://www.google.com/search?'+ args

            '''
            cloak = "https://crawlera.p.mashape.com/fetch"
            headers = {"X-Mashape-Key": "mEol4XmA3QmshtYIjvaaqvts9kyOp1DwvVvjsnoN02b6eKv98h"}
            r = requests.get(cloak, params={'url':url}, headers=headers)
            '''
            r = Crawlera()._get(url)
            res = res.append(self._results_html_to_df(r.text))
        return res

    def _results_html_to_df(self, search_result_html):
        leads = pd.DataFrame()
        listings = BeautifulSoup(search_result_html).findAll('li',{'class':'g'})
        for lead in listings:
            if lead == None: continue
            #print lead
            link_text = lead.find('h3').text
            link = lead.find('a')['href'].split('=')[1].split('&')[0]
            link_span = lead.find(attrs={'class':'st'})
            link_span = link_span.text if link_span else ""
            url = lead.find('cite')
            url = url.text if url else ""
            title = lead.find('div',{'class':'slp'})
            title = title.text if title else ""
            leads = leads.append(dict(zip(['link_text','url','title','link_span','link'],
                                      [link_text, url,title,link_span, link])),
                             ignore_index=True)
        return leads
    def _google_df_to_linkedin_df(self, results):
        if results.empty: return results
        final = pd.DataFrame()
        results =  results.reset_index().drop('index', 1)
        final['name'] = [name.split('|')[0].strip().split(',')[0]
                         for name in results.link_text]
        final['locale']  = [name.split('-')[0].strip()
                            for name in results.title]
        final['company']  = [name.split(' at ')[-1].strip()
                              if " at " in name else ""
                             for name in results.title]
        final['title']  = [name.split(' at ')[0].split('-')[-1].strip()
                             for name in results.title]
        final['linkedin_url'] = results.link.tolist()
        return final
