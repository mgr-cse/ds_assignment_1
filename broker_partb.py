from flask import Flask
from flask import request
import threading
from flask_sqlalchemy import SQLAlchemy

username = 'mattie'
password = 'password'
database = 'psqlqueue'
db_port = '5432'

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://{username}:{password}@localhost:{db_port}/{database}"

# # queue database structures
db = SQLAlchemy(app)
 
class Producer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    
class Consumer(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    offset = db.Column(db.Integer, nullable=False)

class Topic(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    producers = db.relationship('Producer', backref='topic')
    consumers = db.relationship('Consumer', backref='topic')
    messages  = db.relationship('Message', backref='topic')

class Message(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    topic_id = db.Column(db.Integer, db.ForeignKey('topic.id'))
    message_content = db.Column(db.String(255))

# debugging functions
def print_thread_id():
    print('Request handled by worker thread:', threading.get_native_id())

def return_message(status:str, message=None):
    content = dict()
    content['status'] = status
    if message is not None:
        content['message'] = message
    return content

# functions for handelling each endpoint

@app.route('/topics', methods=['POST'])
def topic_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')

    # parse content
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic']
    except:
        return return_message('failure', 'Error While Parsing json')
    
    # database
    try:
        if Topic.query.filter_by(name=topic_name).first() is not None:
            return return_message('failure', 'Topic already exists')  
        
        topic = Topic(name=topic_name)
        db.session.add(topic)
        db.session.commit()
    except:
        db.session.rollback()
        return return_message('failure', 'Error while querying/comitting to database')
    
    return return_message('success', 'topic ' + topic.name + ' created sucessfully')

@app.route('/topics', methods=['GET'])
def topic_get_request():
    print_thread_id()
    topics_list = []
    try:
        # database
        topics = Topic.query.all()
        for t in topics:
            topics_list.append(t.name)
        return return_message('success', topics_list)
    except: 
        return return_message('failure', 'Error while listing topics')

@app.route('/producer/register',methods=['POST'])
def producer_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    # parsing
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic']
    except:
        return return_message('failure', 'Error while parsing request')
        
    # query
    try:
        topic = Topic.query.filter_by(name=topic_name).first()
        if topic is None:
            return return_message('failure', 'Topic does not exist')

        producer = Producer(topic_id=topic.id)
        db.session.add(producer)
        db.session.commit()
        return {
            "status": "success",
            "producer_id": producer.id
        }
    except:
        db.session.rollback()
        return return_message('Failure','Error while querying/commiting database')
