from parse import Parse
import requests

class Webhook:
    def _post(self, api_key, data):
        qry = {'where':json.dumps('api_key':api_key, 'data':data)}
        urls = Parse().get('Webhook', qry).json()['results']
        for url in urls:
            headers = {'content-type': 'application/json'}
            r = requests.post(url, data=json.dumps(data), headers=headers)
        
