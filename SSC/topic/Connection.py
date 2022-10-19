'''
Created on 20221019
Update on 20221019
@author: Eduardo Pagotto
'''

from typing import List
import redis

from SSC.topic import Producer, Consumer
from SSC.topic.PubSub import Publish, Subscribe
from SSC.topic.QueueProdCons import QueueConsumer, QueueProducer

class Connection(object):
    def __init__(self, url: str) -> None:
        # redis://localhost?db=0
        self.__redis = redis.Redis.from_url(url)

    def ping(self) -> bool:
        return self.__redis.ping()

    def create_publish(self, topic : str) -> Producer:
        return Publish(self.__redis, topic)

    def create_subscribe(self, topics : List[str]) -> Consumer:
        return Subscribe(self.__redis, topics)

    def create_producer(self, queue : str)-> Producer:
        return QueueProducer(self.__redis, queue)

    def create_consumer(self, names : List[str]) -> Consumer:
        return QueueConsumer(self.__redis, names)