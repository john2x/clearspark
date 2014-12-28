import requests
import json
import time

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
            # if not_found: search google for it 
            return "not found"

    def _normalize_name(self, name):
        data = {'q':name, 'apiKey':'edbdfddbff83c6d8'}
        r = requests.get('https://api.fullcontact.com/v2/name/normalizer.json',params=data)
        print r.json()
        if 'nameDetails' in r.json().keys():
            return r.json()['nameDetails']['fullName']
        else:
            return name
