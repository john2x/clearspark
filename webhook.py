from parse import Parse
import requests
import json

class Webhook:
    def _post(self, api_key, data):
        qry = {'where':json.dumps({'api_key':api_key})}
        webhooks = Parse().get('Webhook', qry).json()['results']
        print webhooks, api_key
        for hook in webhooks:
            headers = {'content-type': 'application/json'}
            r = requests.post(hook['url'], data=json.dumps(data), headers=headers)
            print 'webhook', r, data, r.text
        
