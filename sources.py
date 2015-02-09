from queue import RQueue
from google import Google
import pandas as pd
import pythonwhois
from smtp import SMTP
from bs4 import BeautifulSoup
from press_sources import BusinessWire
import requests
from press_sources import PRNewsWire
from crawl import CompanyEmailPatternCrawl
from email_guess_helper import EmailGuessHelper
import time
from fullcontact import FullContact
import pystache
from splinter import Browser

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class Sources:
    def _google_span_search(self, domain):
      queue = "google-span-"+domain
      qry_1 = '("media contact" OR "media contacts" OR "press release") "@{0}"'
      qry_1 = qry_1.format(domain)
      qry_2 = '"email * * {0}"'.format(domain)
      first = Google().ec2_search(qry_1)
      second = Google().ec2_search(qry_2)

      q.enqueue(Sources()._google_cache_search, domain, first.link)
      q.enqueue(Sources()._google_cache_search, domain, second.link)

      first = first[first.link_span.str.contains('@')]
      second = second[second.link_span.str.contains('@')]
      emails = [[email for email in span.split() if "@" in email] 
                for span in first.append(second).link_span]
      emails = pd.Series(emails).sum()
      emails = self._research_emails(emails)
      CompanyEmailPatternCrawl()._persist(emails, "google_span_search")

    def _deduce_email_pattern(self, full_name, email):
        person = EmailGuessHelper()._name_to_email_variables(full_name)
        person['domain'] = email.split('@')[-1]
        for pattern in EmailGuessHelper()._patterns():
            _email = pystache.render(pattern, person)
            if email.lower() == _email.lower():
                person['pattern'], person['email'] = pattern, email
                return person               

    def _research_emails(self, emails):
        _emails = pd.DataFrame()
        for email in emails:
            # if -, ., _       | clean emails
            full_name = FullContact()._person_from_email(email)
            print email, full_name
            if type(full_name) is str: continue
            full_name = full_name['contactInfo']['fullName']
            person = EmailGuessHelper()._name_to_email_variables(full_name)
            person['domain'] = email.split('@')[-1]
            for pattern in EmailGuessHelper()._patterns():
                _email = pystache.render(pattern, person)
                if email.lower() == _email.lower():
                    person['pattern'], person['email'] = pattern, email
                    _emails = _emails.append(person, ignore_index=True)
        return _emails

    def _google_cache_search(self, domain, links):
        all_emails = []
        for link in links:
            if "lead411" in link: continue
            html = Google().ec2_cache(link)
            links = BeautifulSoup(html).find_all('a')
            links = [link['href'] for link in links if 'href' in link.attrs] 
            links = [link.split('mailto:')[-1]
                     for link in links if 'mailto:' in link and "@"+domain in link]
            text = BeautifulSoup(html).text
            emails = [word for word in text.split() if "@"+domain in word]
            all_emails = all_emails + emails + links
        emails = self._research_emails(all_emails)
        CompanyEmailPatternCrawl()._persist(emails, "google_cache_search")

    def _whois_search(self, domain):
        # TODO - fix this
        results = pythonwhois.get_whois(domain)
        try: emails = pythonwhois.get_whois(domain)
        except: return pd.DataFrame()
        emails = filter(None, results['contacts'].values())
        emails = pd.DataFrame(emails)
        emails['domain'] = domain
        for index, row in emails.iterrows():
            name = FullContact()._normalize_name(row['name'])
            email = row.email.strip()
            pattern = EmailGuessHelper()._find_email_pattern(name, row.email)
            emails.ix[index, 'pattern'] = pattern
        CompanyEmailPatternCrawl()._persist(emails, "whois_search")

    def _press_search(self, domain):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))
        #job_queue_lol = objectId+str(arrow.now().timestamp)
        pw = pw if not pw.empty else pd.DataFrame(columns=["link"])
        bw = pw if not bw.empty else pd.DataFrame(columns=["link"])
        queue = "press-check-"+domain
        '''
        for link in pw.link: 
            job = q.enqueue(PRNewsWire()._email, domain, link, timeout=3600)
            job.meta[queue] = True; job.save()

        for link in bw.link: 
            job = q.enqueue(BusinessWire()._email, domain, link, timeout=3600)
            job.meta[queue] = True; job.save()
        '''
        bw = pd.concat([BusinessWire()._email(domain, link) for link in bw.link])
        pw = pd.concat([PRNewsWire()._email(domain, link) for link in pw.link])
        emails = bw.append(pw)
        CompanyEmailPatternCrawl()._persist(emails, "press_search")

    def _zoominfo_search(self, domain):
        qry = 'site:zoominfo.com/p/ "@{0}"'.format(domain)
        queue = "zoominfo-check-"+domain
        test = Google().search(qry, 5)
        res = [[word.lower() for word in link.split() if "@" in word]
                for link in test[test.link_span.str.contains('@')].link_span]
        test.ix[test.link_span.str.contains('@'), 'email'] = res
        test = test[test.email.notnull()]
        test['name'] = [link.split('|')[0].strip() for link in test.link_text]
        emails = test
        emails['domain'] = domain
        patterns = []
        for index, row in emails.iterrows():
            name = FullContact()._normalize_name(row['name']).strip()
            email = row.email.strip()
            if email[-1] is ".": email = email[:-1]
            pattern = EmailGuessHelper()._find_email_pattern(name, email)
            patterns.append(pattern)

        emails['pattern'] = patterns
        CompanyEmailPatternCrawl()._persist(emails, "zoominfo_search")

    def _mx_server_check(self, name, domain):
        print "START MX SERVER CHECK"
        mx_servers = SMTP()._mx_servers(domain)
        print mx_servers
        smtp = SMTP()._smtp_auth(mx_servers)
        print smtp
        try: 
            mx_servers = SMTP()._mx_servers(domain)
            smtp = SMTP()._smtp_auth(mx_servers)
        except: return pd.DataFrame()

        print "vars"
        prospect = EmailGuessHelper()._name_to_email_variables(name)
        print prospect
        prospect['domain'] = domain
        print prospect
        results = pd.DataFrame()
        print prospect
        for pattern in EmailGuessHelper()._patterns():
            email = pystache.render(pattern, prospect)
            try: result = smtp.docmd('rcpt to:<{0}>'.format(email))
            except: continue
            prospect['smtp_result'] = result[1]
            prospect["pattern"] = pattern
            print result
            if 'OK' in result[1]: 
                prospect['email'] = email
                results = results.append(prospect, ignore_index=True)
        # persist to parse
        CompanyEmailPatternCrawl()._persist(results, source="mx_check")
        return results

    #TODO - finish integrating these data sources
    def _jigsaw_search(self, company_name):
        browser = Browser('chrome')
        browser.visit('https://connect.data.com/login')
        browser.find_by_css('#j_username').first.fill('robin@customerohq.com')
        browser.find_by_css('#j_password').first.fill('951562nileppeZ')
        browser.find_by_css('#login_btn').first.click()
        browser.find_by_css('#homepageSBS').first.fill(company_name)
        browser.find_by_css('.homepage-search-icon').first.click()

        if len(browser.find_by_css('.companyName')):
            time.sleep(1)
            browser.find_by_css('.companyName').first.click()
            time.sleep(1)
            browser.find_by_css('.company-counts > a').first.click()
            time.sleep(1)
            #browser.find_by_name('directDial').first.click()
            time.sleep(1)
            browser.find_by_css('.td-name > a').first.click()
            #browser.find_by_css('td.name').first.click()
            time.sleep(1)
            try:
              ''' browser.find_by_css('#getDetailsLink').first.click() '''
            except:
              ''' lol '''
            name = browser.find_by_css('.businesscard-contactinfo-name')[0].text
            emails = browser.find_by_css('.businesscard-contactinfo-email')[0].text
            print name, emails
            print self._deduce_email_pattern(name, emails)
        
    def _domain_harvest(self, domain):
        ''' Crawl Domain And Extract '''

    # TODO - later
    def _personal_mongo_check(self, domain):
        ''' Personal DB Check '''

    def _linkedin_login_search(self, domain):
        ''' linkedin login search '''

    def _mass_linkedin_login_search(self, domain):
        ''' get employees then linkedin login search '''

    def _mass_mx_server_check(self, domain):
        ''' get employees then mx server check '''

