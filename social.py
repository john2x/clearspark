from splinter import Browser
from parse import Parse
from google import Google
import json
import requests
from bs4 import BeautifulSoup
from crawl import CompanyInfoCrawl
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

    def _tweets(self, html, api_key):
        #html = Google().cache("https://twitter.com/guidespark")
        tw = BeautifulSoup(html)
        tweets = []
        for tweet in tw.find_all("div",{"class":"ProfileTweet"}):
            text = tweet.find("p",{"class":"ProfileTweet-text"}).text
            hashtags = [hashtag["href"] for hashtag in tweet.find_all("a",{"class":"twitter-hashtag"})]
            mentions = ["twitter.com"+reply["href"] 
                        for reply in tweet.find_all("a",{"class":"twitter-atreply"})]
            links = [link["href"] 
                    for link in tweet.find_all("a",{"class":"twitter-timeline-link"})]
            photos = [img["src"]
                      for img in tweet.find_all("img",{"class":"TwitterPhoto-mediaSource"})]
            tweet = {"text":text,"hashtags":hashtags,"mentions":mentions,
                     "links":links, "photos":photos}
            tweets.append(tweet)
            CompanyExtraInfoCrawl()._persist(tweet, "tweets")
        tweets = pd.DataFrame(tweets)
      
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
        url = df.link.tolist()[0]
        html = requests.get(url).text
        val = self._html_to_dict(html)
        val["company_name"] = name
        CompanyInfoCrawl()._persist(val, "twitter", api_key)

    def _html_to_dict(self, html):
        html = BeautifulSoup(html)
        logo = html.find('img',{'class':'ProfileAvatar-image '})['src']
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
            utime = post.find("abbr",{"class":"livetimestamp"})["data-utime"]
            post_text = post.find("div",{"class":"userContent"}).text
            if post.find('div',{'class':'_3ekx'}):
                try:
                    link_url = post.find('div',{'class':'_3ekx'}).find('a')["href"]
                    link_url = urllib.unquote(link_url.split("l.php?u=")[-1])
                    link_img = post.find('img',{"class":"scaledImageFitWidth"})["src"]
                    link_title = post.find('div',{'class':'mbs'}).text
                    link_summary = post.find('div',{'class':'_6m7'}).text
                except:
                    print post_text
                    break
            post = {"utime":utime, "post_text":post_text, "link_url":link_url,
                    "link_img":link_img, "link_title":link_title, 
                    "link_summary":link_summary}
            #TODO - company_name, domain
            posts.append(post)
        return posts
        posts = pd.DataFrame(posts)      

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
        url = df.link.tolist()[0]
        html = Google().cache(url)
        #browser = Browser('phantomjs')
        #browser.visit(url)
        val = self._html_to_dict(html)
        val["company_name"] = name
        CompanyInfoCrawl()._persist(val, "facebook", api_key)

    def _scrape_posts(self, html):
        ''' '''

    def _html_to_dict(self, html):
        html = BeautifulSoup(html)
        logo = html.find('img',{'class':'profilePic'})['src']
        link = html.find('a',{'class':'profileLink'})['href']
        name = html.find('span',{'itemprop':'name'})
        name = name.text if name else ""
        likes = html.find('span',{'id':'PagesLikesCountDOMID'})
        data = {'logo':logo, 'handle':link, 'name':name}
        if likes:
          data["likes"] = likes.text.split(' likes')[0].replace(',')
          data["likes"] = int(data["likes"])
        return data
        
