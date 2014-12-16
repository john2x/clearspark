from queue import Queue

class Sources:
    def _google_span_search(self, domain):
      queue = "google-span-"+domain
      qry_1 = '("media contact" OR "media contacts" OR "press release") "@{0}"'
      qry_1 = qry_1.format(domain)
      qry_2 = '"email * * {0}"'.format(domain)
      job_1 = q.enqueue(Google().ec2_search, qry_1)
      job_2 = q.enqueue(Google().ec2_search, qry_2)
      while not Queue()._has_completed(queue): 
          results = Queue()._results(queue)
          self._google_cache_search(domain, results.link_span)
          # scrape all emails
          # fullcontact / clearbit to figure out who it is
          # start google cache search
          return results

    def _google_cache_search(self, domain, links):
        for link in links:
            job = q.enqueue(Google()._ec2_cache, link)
            job.meta['domain'] = domain; job.meta['google-cache-'+domain] =True 
            job.save()
        # fullcontact / clearbit to figure out who email is
        queue = "google-cache-"+domain
        while not Queue()._has_completed(queue): return Queue()._results(queue)

    def _whois_search(self, domain):
        try: results = pythonwhois.get_whois(domain)
        except: return pd.DataFrame()
        results = filter(None, results['contacts'].values())
        results = pd.DataFrame(results)
        results['domain'] = domain
        return results

    def _mx_server_check(self, name, domain):
        try: 
            mx_servers = SMTP()._mx_servers(domain)
            smtp = SMTP()._smtp_auth(mx_servers)
        except: return pd.DataFrame()

        prospect = EmailGuessHelper()._name_to_email_variables(name)
        prospect['domain'] = domain
        results = pd.DataFrame()
        for pattern in EmailPattern()._patterns():
            email = pattern.format(**prospect)
            try: result = smtp.docmd('rcpt to:<{0}>'.format(email))
            except: continue
            prospect['smtp_result'] = result[1]
            print result
            if 'OK' in result[1]: 
                prospect['email'] = email
                results = results.append(prospect, ignore_index=True)
        return results
        
    def _press_check(self, domain):
        pw = Google().search('"{0}" site:prnewswire.com'.format(domain))
        bw = Google().search('"{0}" site:businesswire.com'.format(domain))
        job_queue_lol = objectId+str(arrow.now().timestamp)
        pw = pw if not pw.empty else pd.DataFrame(columns=["link"])
        bw = pw if not bw.empty else pd.DataFrame(columns=["link"])
        queue = "press-check-"+domain

        for link in pw.link: 
            job = q.enqueue(PRNewsWire()._email, domain, link, timeout=3600)
            job.meta[queue] = True; job.save()

        for link in bw.link: 
            job = q.enqueue(BusinessWire()._email, domain, link, timeout=3600)
            job.meta[queue] = True; job.save()

        while not Queue()._has_completed(queue): return Queue()._results(queue)

    def zoominfo_harvest(self, domain):
        qry = 'site:zoominfo.com/p/ "@{0}"'.format(domain)
        queue = "zoominfo-check-"+domain
        job = q.enqueue(Google().ec2_search, domain)
        job.meta[queue] = True; job.save()
        while not Queue()._has_completed(queue): 
            pass
            # get results
            # filter for ones with @domain
            # return results

    #TODO - finish integrating these data sources
    def data_com(self, domain):
        ''' Check Rest API''' 
        # data.com browser automation
        
    def domain_harvest(self, domain):
        ''' Figure Out Domain And Extract'''

    def _personal_mongo_check(self, domain):
        ''' Personal DB Check '''

