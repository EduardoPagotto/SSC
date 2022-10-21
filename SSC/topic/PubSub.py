'''
Created on 20221019
Update on 20221022
@author: Eduardo Pagotto
'''

from typing import Any, List
import redis

from SSC.topic import Producer, Consumer

class Publish(Producer):
    def __init__(self, url : str, topic : str) -> None:

        self.__redis = redis.Redis.from_url(url)
        self.__topic = topic

    def send(self, item: Any) -> int:
        return self.__redis.publish(self.__topic, item)    

class Subscribe(Consumer):
    def __init__(self,  url : str, topics : List[str]) -> None:

        self.__redis = redis.Redis.from_url(url)
        self.__pubsub = self.__redis.pubsub()
        self.__pubsub.subscribe(*topics)

    def receive(self, timeout : float = 0) -> dict[str, Any] | None:
        return self.__pubsub.get_message(timeout=timeout)

    def nack(self, queue_name : str, register : Any) -> int:
        return self.__redis.publish(queue_name, register)