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

from rq import Queue
from worker import conn
q = Queue(connection=conn)

#class EmailSources:
class FullContact:
    def _person_from_email(self, email):
        data = {'email':email, 'apiKey':'edbdfddbff83c6d8'}
        r = requests.get('https://api.fullcontact.com/v2/person.json',params=data)
        print r.status_code, r.json()
        while r.status_code == 202:
            time.sleep(1)
            r = requests.get('https://api.fullcontact.com/v2/person.json',params=data)
            print r.status_code, r.json()
        if r.status_code == 200:
            return r.json()
        else:
            return "not found"

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
      CompanyEmailPatternCrawl()._persist("Google Span Search", emails)

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
                _email = pattern.format(**person)
                if email.lower() == _email.lower(): break
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
        CompanyEmailPatternCrawl()._persist("Google Cache Search", emails)

    def _whois_search(self, domain):
        results = pythonwhois.get_whois(domain)
        try: emails = pythonwhois.get_whois(domain)
        except: return pd.DataFrame()
        emails = filter(None, results['contacts'].values())
        emails = pd.DataFrame(emails)
        emails['domain'] = domain
        # guess email patterns
        for index, row in emails.iterrows():
            pattern = EmailGuessHelper()._find_email_pattern(row.name, row.email)
            emails.ix[index, 'pattern'] = pattern
            
        CompanyEmailPatternCrawl()._persist("Whois Search", emails)

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
        CompanyEmailPatternCrawl()._persist("Press Search", emails)

    def _zoominfo_search(self, domain):
        qry = 'site:zoominfo.com/p/ "@{0}"'.format(domain)
        queue = "zoominfo-check-"+domain
        test = Google().search(qry, 5)
        res = [[word.lower() for word in link.split() if "@" in word]
                for link in test[test.link_span.str.contains('@')].link_span]
        test.ix[test.link_span.str.contains('@'), 'emails'] = res
        test = test[test.emails.notnull()]
        test['name'] = [link.split('|')[0].strip() for link in test.link_text]
        CompanyEmailPatternCrawl()._persist("Zoominfo Search", test)

    def _mx_server_check(self, name, domain):
        print "START MX SERVER CHECK"
        mx_servers = SMTP()._mx_servers(domain)
        smtp = SMTP()._smtp_auth(mx_servers)
        try: 
            mx_servers = SMTP()._mx_servers(domain)
            smtp = SMTP()._smtp_auth(mx_servers)
        except: return pd.DataFrame()

        prospect = EmailGuessHelper()._name_to_email_variables(name)
        prospect['domain'] = domain
        results = pd.DataFrame()
        print prospect
        for pattern in EmailPattern()._patterns():
            email = pattern.format(**prospect)
            try: result = smtp.docmd('rcpt to:<{0}>'.format(email))
            except: continue
            prospect['smtp_result'] = result[1]
            print result
            if 'OK' in result[1]: 
                prospect['email'] = email
                results = results.append(prospect, ignore_index=True)
        # persist to parse
        return results

    #TODO - finish integrating these data sources
    def _linkedin_login_search(self, domain):
        ''' linkedin login search '''

    def _mass_linkedin_login_search(self, domain):
        ''' get employees then linkedin login search '''

    def _mass_mx_server_check(self, domain):
        ''' get employees then mx server check '''

    def _jigsaw_search(self, domain):
        ''' data.com browser automation '''
        
    def _personal_mongo_check(self, domain):
        ''' Personal DB Check '''

    def _domain_harvest(self, domain):
        ''' Crawl Domain And Extract '''
