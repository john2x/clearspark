from splinter import Browser
import pandas as pd
from parse import Parse
from google import Google
import json
import urllib
import requests
from bs4 import BeautifulSoup
from crawl import CompanyInfoCrawl, CompanyExtraInfoCrawl
import tldextract

class Twitter:
    def _signal(self, link, api_key=""):
        html = Google().cache(link)
        info = self._html_to_dict(html)
        tweets = self._tweets(html)
        CompanyInfoCrawl()._persist(info, "twitter", api_key)
        for tweet in tweets:
          CompanyExtraInfoCrawl()._persist(tweet, "tweets", api_key)

    def _recent(self):
        df = Google().search("site:twitter.com", period="h")
        for link in df.link:
            q.enqueue(Twitter()._signal, link)

    def _daily_news(self, domain, api_key="", name=""):
        df = Google().search('site:twitter.com {0}'.format(domain))
        link = df.link.tolist()[0]
        html = Google().cache(link)
        tweets = self._tweets(html, api_key)
        data = {"data":tweets, "company_name":name, "domain":domain}
        CompanyExtraInfoCrawl()._persist(data, "tweets")

    def _tweets(self, html, api_key):
        #html = Google().cache("https://twitter.com/guidespark")
        tw = BeautifulSoup(html)
        tweets = []
        for tweet in tw.find_all("div",{"class":"ProfileTweet"}):
            timestamp = tweet.find("span", {"class":"js-short-timestamp"})["data-time"]
            text = tweet.find("p",{"class":"ProfileTweet-text"}).text
            _hashtags = tweet.find_all("a",{"class":"twitter-hashtag"})
            hashtags = [hashtag["href"] for hashtag in _hashtags]
            _mentions = tweet.find_all("a",{"class":"twitter-atreply"})
            mentions = ["twitter.com"+reply["href"] for reply in _mentions]
            _links = tweet.find_all("a",{"class":"twitter-timeline-link"})
            links = [link["href"] for link in _links]

            _imgs = tweet.find_all("img",{"class":"TwitterPhoto-mediaSource"})
            photos = [img["src"] for img in _imgs]

            tweet = {"text":text,"hashtags":hashtags,"mentions":mentions,
                     "links":links, "photos":photos, "timestamp":timestamp}
            #TODO - add timestamp
            tweets.append(tweet)
        #tweets = pd.DataFrame(tweets)
        return tweets
      
    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:twitter.com {0}'.format(domain))
        for url in df.link:
            r = requests.get(url).text
            link = BeautifulSoup(r).find('span',{'class':'ProfileHeaderCard-urlText'}).text.strip()
            if domain not in link: continue
            val = self._html_to_dict(r)
            break
        val["company_name"] = name
        val["domain"] = domain
        CompanyInfoCrawl()._persist(val, "twitter", api_key)

    def _company_profile(self, name, api_key=""):
        df = Google().search('site:twitter.com {0}'.format(name))
        if df.empty: return df
        url = df.link.tolist()[0]
        html = requests.get(url).text
        val = self._html_to_dict(html)
        val["company_name"] = name
        CompanyInfoCrawl()._persist(val, "twitter", api_key)

    def _html_to_dict(self, html):
        html = BeautifulSoup(html)
        logo = html.find('img',{'class':'ProfileAvatar-image '})
        logo = logo['src'] if logo else ""
        link = html.find('h2',{'class':'ProfileHeaderCard-screenname'})
        link = link.text.strip().lower()
        link = "twitter.com/"+link.split('@')[-1]
        print link
        name = html.find('h1',{'class':'ProfileHeaderCard-name'})
        name = name.text.strip().lower()
        # add company_name
        return {'logo':logo, 'handle':link, 'name':name}

class Facebook:
    def _signal(self, link, api_key=""):
        html = Google().cache(link)
        info = self._html_to_dict(html)
        posts = self._posts(html)
        CompanyInfoCrawl()._persist(info, "facebook", api_key)
        for post in posts:
          CompanyExtraInfoCrawl()._persist(post, "facebook_posts", api_key)

    def _recent(self):
        df = Google().search("site:facebook.com", period="h")
        for link in df.link:
            q.enqueue(Facebook()._signal, link)

    def _posts(self, html, api_key=""):
        # &tbs=qdr:h,sbd:1 # &tbs=qdr:d,sbd:1 # &tbs=qdr:w,sbd:1
        # &tbs=qdr:m,sbd:1 # &tbs=qdr:y,sbd:1
        #html = Google().cache("https://www.facebook.com/Socceroos")
        fb = BeautifulSoup(html)

        posts = []
        for post in fb.find_all("div",{"class":"userContentWrapper"}):
            #utime = post.find("abbr",{"class":"livetimestamp"})
            utime = post.find("abbr")
            utime = utime["data-utime"] if utime else ""
            post_text = post.find("div",{"class":"userContent"}).text
            _post = {"timestamp":utime, "post_text":post_text}
            if post.find('div',{'class':'_3ekx'}):
              link_url = post.find('div',{'class':'_3ekx'}).find('a')["href"]
              link_url = urllib.unquote(link_url.split("l.php?u=")[-1])
              link_img = post.find('img',{"class":"scaledImageFitWidth"})["src"]
              link_title = post.find('div',{'class':'mbs'}).text
              link_summary = post.find('div',{'class':'_6m7'}).text
              _post["link_url"], _post["link_img"] = link_url, link_img, 
              _post["link_title"] = link_title
              _post["link_summary"] = link_summary
            posts.append(_post)
        return posts

    def _daily_news(self, domain, api_key="", name=""):
        df = Google().search('site:facebook.com {0}'.format(domain))
        link = df.link.tolist()[0]
        html = Google().cache(link)
        posts = Facebook()._posts(html)
        posts = pd.DataFrame(posts).fillna("")
        data = {"data":posts.to_dict("r"), "domain":domain, "company_name":name}
        CompanyExtraInfoCrawl()._persist(data, "facebook_posts", api_key)

    def _domain_search(self, domain, api_key="", name=""):
        df = Google().search('site:facebook.com {0}'.format(domain))
        for url in df.link:
            #browser = Browser('phantomjs')
            #browser.visit(url)
            # html = browser.html
            html = Google().cache(url)
            if domain not in BeautifulSoup(html).text: continue
            val = self._html_to_dict(html)
            val["company_name"] = name
            val["domain"] = domain
            CompanyInfoCrawl()._persist(val, "facebook", api_key)
            break

    def _company_profile(self, name, api_key=""):
        df = Google().search('site:facebook.com {0}'.format(name))
        if df.empty: return df
        url = df.link.tolist()[0]
        html = Google().cache(url)
        #browser = Browser('phantomjs')
        #browser.visit(url)
        val = self._html_to_dict(html)
        print val
        val["company_name"] = name
        CompanyInfoCrawl()._persist(val, "facebook", api_key)

    def _scrape_posts(self, html):
        ''' '''

    def _html_to_dict(self, html):
        html = BeautifulSoup(html)
        logo = html.find('img',{'class':'profilePic'})
        link = html.find('a',{'class':'profileLink'})
        link = link["href"] if link else ""
        logo = logo["src"] if logo else ""
        name = html.find('span',{'itemprop':'name'})
        name = name.text if name else ""
        likes = html.find('span',{'id':'PagesLikesCountDOMID'})
        data = {'logo':logo, 'handle':link, 'name':name}
        if likes:
          data["likes"] = likes.text.split(' likes')[0].replace(',', "")
          data["likes"] = int(data["likes"])
        return data
        
