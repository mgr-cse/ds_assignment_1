import sys
import requests
import json

endpoint = sys.argv[1]
req_type = sys.argv[2]
params   = sys.argv[3]

try:
   params = json.loads(params) 
except:
    print('Error while parsing json')
    exit(-1)

url = 'http://localhost:5000/'
url = url + endpoint

res = None
if req_type == 'get':
    res = requests.get(url, params=params)
else:
    res = requests.post(url, json=params)
if res.ok:
    try:
         print(res.json())
    except:
        print('exept occured')

