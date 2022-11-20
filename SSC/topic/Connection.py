'''
Created on 20221019
Update on 20221120
@author: Eduardo Pagotto
'''

from typing import List
import redis
from SSC.topic.QueueProdCons import QueueConsumer, QueueProducer

class Connection(object):
    def __init__(self, url: str) -> None:
        # redis://localhost?db=0
        self.url = url

    def ping(self) -> bool:
        r = redis.Redis.from_url(self.url)
        return r.ping()

    def create_producer(self, queue : str, producer : str = '')-> QueueProducer:
        return QueueProducer(self.url, queue, producer)

    def create_consumer(self, names : List[str]) -> QueueConsumer:
        return QueueConsumer(self.url, names)