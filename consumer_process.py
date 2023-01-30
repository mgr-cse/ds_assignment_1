from consumer import Consumer
import requests

import sys
import time

import threading

poll_interval = 0.2

name = sys.argv[1]
topics = sys.argv[2]

topics = topics.split(',')
print(topics)

consumer = Consumer('localhost', 5000, name)

for t in topics:
    consumer.register(t)


consumer.run(60, 'tests/log/', 0.01)