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
