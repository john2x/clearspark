import smtplib

class SMTP:
    def _mx_servers(domain):
        answers = dns.resolver.query(domain, 'MX')
        results = [(str(rdata.exchange), rdata.preference) 
                   for rdata in answers]
        return results
    def _smtp_auth(self, mx_servers):
        smtp = smtplib.SMTP(mx_servers[0][0], 25)
        smtp.helo()
        smtp.docmd('mail from:<labnol@labnol.org>')
        return smtp
    
    def find_valid_email_patterns(self, prospect, smtp):
        for pattern in _patterns():
            email = pattern.format(**prospect)
            try:
                result = smtp.docmd('rcpt to:<{0}>'.format(email))
            except:
                continue
            print result
            if 'OK' in result[1]:
                print email
