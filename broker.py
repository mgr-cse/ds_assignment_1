from flask import Flask
from flask import request
from threading import Lock
import json


class TopicQueue:
    def __init__(self, topic_name):
        # each topic queue has its own lock to ensure broker ordering
        self.lock = Lock()
        self.messages = []
        self.topic_name = topic_name


app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, Distributed Queue!'

@app.route('/topics', methods=['POST'])
def topic_register_request():
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/json':    
        receive = request.json
        try:
            global queues
            topic_name = receive['topic_name']
            
            # lock the queues, we don't want to return the wrong status
            # amd perform an unecessary insert
            with queues_lock:
                if topic_name not in queues:
                    queues[topic_name] = TopicQueue(topic_name)
                    return {
                            "status": "success",
                            "message": "Topic " + topic_name + " created sucessfully"
                        }
                else: 
                    return {
                        "status": "failure",
                        "message": "Topic " + topic_name + " already exists" 
                    }

        except:
            return {
                "status": "failure",
                "message": "error while parsing request"
            }
    
    return {
       "status": "failure",
       "message": "Content-Type not supported!" 
    }

@app.route('/topics', methods=['GET'])
def topic_get_request():
    topics = []
    with queues_lock:
        for key in queues:
            topics.append(key)
    return {
        "status": "success",
        "topics": topics
    }



if __name__ == "__main__":
    # queuing data structures
    queues_lock = Lock()
    queues = dict()

    app.run(debug=True, threaded=True, processes=1)