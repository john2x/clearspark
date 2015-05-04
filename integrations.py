import pandas as pd
from parse import Parse, Prospecter
from simple_salesforce import Salesforce
import gdata
import json
import requests
import gdata.gauth
import gdata.contacts.client
import arrow

class Integrations:
    def _salesforce_import(self, session_id, instance, user, user_company):
        #print session_id, instance, user, user_company
        #SESSION_ID = "00Dj0000001neXP!AQUAQIbUn9RsdTZH6MbFA7qaPtDovNU75.fOC6geI_KnEhJKyUzk2_yFx2TXgkth7zgFuJThY6qZQwH7Pq4UtlcW.Cq0aHt1"
        print instance
        print instance.replace("https://","")
        sf = Salesforce(instance=instance.replace("https://",""),
                        session_id=session_id)
        lol = sf.query_all("SELECT Id, Name, Email FROM Contact")
        sf = pd.DataFrame(pd.DataFrame(lol).records.tolist())
        sf = sf[["Name","Email"]]
        sf.columns = ["name","email"]
        sf = sf.dropna()
        sf["domain"] = [i.split("@")[-1] if i else "" for i in sf.email]
        sf["source"] = "salesforce"
        sf["db_type"] = "crm"
        sf["user"] = [Parse()._pointer("_User", user) for i in sf.index] 
        sf["user_company"] = [Parse()._pointer("UserCompany",user_company) 
                              for i in sf.index]
        Parse()._batch_df_create("UserContact", sf)
        Prospecter()._batch_df_create("UserContact", sf)
        print Prospecter().update("_User/"+user, 
                {"salesforce_integration":arrow.utcnow().timestamp, 
                 "salesforce_token":session_id}).json()

    def _google_contact_import(self, access_token, user, user_company):
        print access_token, user, user_company
        GOOGLE_CLIENT_ID = "1949492796-qq27u1gnqoct2n6p3hctb0cto58qel5i.apps.googleusercontent.com"
        GOOGLE_CLIENT_SECRET = "GpZlpLB66sU5v9SDPnPf-Ov1"
        #access_token = "ya29.aQFZQT43xw5UeOwINZZoOwCa_X1iND9QmWfp1ZJ2laZx1dU6iJomXSmOaUw2bFAM5f8jhWLCrKWWkQ"
        #access_token = "ya29.aQFdBMBQlqXv8RxtyH-qhKxPpNRU7Y_dTQt0Jbt3wFjzlbR-oNbAiYD-mPgQZXyxAW56JDKK7kCADA"
        # GData with access token
        token = gdata.gauth.OAuth2Token(
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scope='https://www.google.com/m8/feeds',
            user_agent='app.testing',
            access_token=access_token)
         
        contact_client = gdata.contacts.client.ContactsClient()
        token.authorize(contact_client)
         
        feed = contact_client.GetContacts()
         
        for entry in feed.entry:
          entry.title.text
          for e in entry.email:
            e.address
         
        # JSON with access token
        contacts = []
        for i in range(0,10):
            index = i*50 if i != 0 else 1
            url = 'https://www.google.com/m8/feeds/contacts/default/full?access_token={0}&alt=json&max-results=50&start-index={1}'
            url = url.format(access_token, index)
            res = requests.get(url).text             
            data = json.loads(res)
            if "entry" not in data["feed"].keys():
                break
            contacts = contacts + data["feed"]["entry"]
            print len(contacts)
            
        contacts_ = []
        for i, row in pd.DataFrame(contacts)[["gd$email","title"]].iterrows():
            #print row["gd$email"][0]["address"], row["title"]["$t"]
            contacts_.append({"email":row["gd$email"][0]["address"],
                             "name": row["title"]["$t"],
                             "domain": row["gd$email"][0]["address"].split("@")[-1]})
        contacts_ = pd.DataFrame(contacts_)
        contacts_["source"] = "gmail"
        contacts_["db_type"] = "inbox"
        contacts_["user"]=[Parse()._pointer("_User", user) for i in contacts_.index]
        contacts_["user_company"] = [Parse()._pointer("UserCompany",user_company) for i in contacts_.index]
        Parse()._batch_df_create("UserContact", contacts_)
        Prospecter()._batch_df_create("UserContact", contacts_)
        print Prospecter().update("_User/"+user, 
                {"google_integration":arrow.utcnow().timestamp, 
                 "google_token":access_token}).json()
        # Bulk Upload Contacts
        # - fields name, email, domain, company_name, company, stuff
