from typing import Dict
import requests
import json
import time
import sys
import threading

class Consumer:
    def __init__(self, host: str, port: int, name: str = '') -> None:
        self.hostname: str = 'http://' + host + ':' + str(port) + '/'
        self.ids : Dict[str, int] = {}
        self.name: str = name
        self.last_message_time = time.time()
    
    def eprint(self, *args, **kwargs):
            print(self.name, *args, file=sys.stderr, **kwargs)
    
    def register(self, topic: str) -> int:
        if topic in self.ids:
            self.eprint('already registered to topic', topic)
            return -1
        try:
            res = requests.post(self.hostname + 'consumer/register', json={"topic":topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        self.ids[topic] = response['consumer_id']
                    else: self.eprint(response)
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return -1

    def dequeue(self, topic: str) -> bool|str:
        if topic not in self.ids:
            self.eprint('not registered for topic', topic)
            return False
        
        cons_id = self.ids[topic]
        try:
            res = requests.get(self.hostname + 'consumer/consume', params={"consumer_id": cons_id, "topic": topic})
            if res.ok:
                try:
                    response = res.json()
                    print(response)
                    if response['status'] == 'success':
                        return response['message'] 
                    else: self.eprint(response['message'])
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected response code', res.status_code)
        except:
            self.eprint('Can not make post request')
        return False
    
    
    def poll(self, topic: str, timeout: float, poll_time:float, poll_after_error:float ,consume_log=sys.stdout):
        while time.time() < self.last_message_time + timeout:
            #print(consumer.name, 'wokeup')
            message = self.dequeue(topic)
            print(message)
            if message == False:
                time.sleep(poll_after_error)
                pass
            else:
                self.last_message_time = time.time()
                consume_log.write(message + '\n')
                time.sleep(poll_time)
            
    def run(self, timeout: float, log_folder: str, poll_time=0.5, poll_after_error=5):
        topic_threads = []
        files = []

        self.last_message_time = time.time()
        for t in self.ids:
            file = open(log_folder + self.name + '_' + t + '.txt', 'w')
            thread = threading.Thread(target=self.poll, args=[t, timeout, poll_time, poll_after_error, file])
            topic_threads.append(thread)
            files.append(file)
            thread.start()
        
        for t in topic_threads:
            t.join()
        for f in files:
            f.close()
    

