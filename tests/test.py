import requests


res = requests.post('http://localhost:5000/topics', json={"topic":"lalala"})
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

res = requests.post('http://localhost:5000/producer/register',json={"topic":"lalala"})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')

res = requests.post('http://localhost:5000/consumer/register',json={"topic":"lalala"})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')

res = requests.post('http://localhost:5000/producer/produce',json={"topic":"lalala", "producer_id":0, "message": "test message1"})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')

res = requests.post('http://localhost:5000/producer/produce',json={"topic":"lalala", "producer_id":0, "message": "test message2"})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')

res = requests.get('http://localhost:5000/consumer/consume',params={"topic":"lalala", "consumer_id":0})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')

res = requests.get('http://localhost:5000/size',params={"topic":"lalala", "consumer_id":0})
if res.ok:
    try:
        print(res.json())
    except:
        print('exept occured')