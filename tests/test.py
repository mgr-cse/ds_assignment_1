import requests


res = requests.post('http://localhost:5000/topics', json={"topic_name":"lalala"})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')


res = requests.get('http://localhost:5000/topics')
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')