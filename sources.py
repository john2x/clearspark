from queue import RQueue
from google import Google
import pandas as pd
import pythonwhois
from smtp import SMTP
from bs4 import BeautifulSoup

from rq import Queue
from worker import conn
q = Queue(connection=conn)

class Sources:
#class EmailSources:
    def _google_span_search(self, domain):
      queue = "google-span-"+domain
      qry_1 = '("media contact" OR "media contacts" OR "press release") "@{0}"'
      qry_1 = qry_1.format(domain)
      qry_2 = '"email * * {0}"'.format(domain)
      first = Google().ec2_search(qry_1)
      second = Google().ec2_search(qry_2)
      first = first[first.link_span.str.contains('@')]
      second = second[second.link_span.str.contains('@')]
      print first
      print second
      # persist

      q.enqueue(Sources()._google_cache_search, domain, first.link)
      q.enqueue(Sources()._google_cache_search, domain, second.link)
      # scrape all emails
      # fullcontact / clearbit to figure out who it is
      # start google cache search
      # return results

    def _google_cache_search(self, domain, links):
        all_emails = []
        for link in links:
            if "lead411" in link: continue
            html = Google().ec2_cache(link)
            links = BeautifulSoup(html).find_all('a')
            links = [link['href'] for link in links 
                      if 'mailto:' in link['href'] and domain in link['href']]
            text = BeautifulSoup(html).text
            emails = [word for word in text.split() if "@"+domain in word]
            all_emails = all_emails + emails + links
        print all_emails
        # fullcontact / clearbit to figure out who email is
        # guess email pattern


    def _whois_search(self, domain):
        results = pythonwhois.get_whois(domain)
        try: results = pythonwhois.get_whois(domain)
        except: return pd.DataFrame()
        results = filter(None, results['contacts'].values())
        results = pd.DataFrame(results)
        results['domain'] = domain
        # persist
        return results

    def _mx_server_check(self, name, domain):
        print "START MX SERVER CHECK"
        # get employees?
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
            print pattern
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
        
    def _press_sources(self, domain):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))
        job_queue_lol = objectId+str(arrow.now().timestamp)
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

        print bw.append(pw)



    def _zoominfo_harvest(self, domain):
        qry = 'site:zoominfo.com/p/ "@{0}"'.format(domain)
        queue = "zoominfo-check-"+domain
        test = Google().search(qry, 5)
        res = [[word.lower() for word in link.split() if "@" in word]
                for link in test[test.link_span.str.contains('@')].link_span]
        test.ix[test.link_span.str.contains('@'), 'emails'] = res
        test = test[test.emails.notnull()]
        test['name'] = [link.split('|')[0].strip() for link in test.link_text]
        print test
        #while not RQueue()._has_completed(queue): pass

    #TODO - finish integrating these data sources
    def data_com(self, domain):
        ''' Check Rest API''' 
        # data.com browser automation
        
    def _personal_mongo_check(self, domain):
        ''' Personal DB Check '''

    def domain_harvest(self, domain):
        ''' Figure Out Domain And Extract'''

