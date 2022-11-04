'''
Created on 20221019
Update on 20221103
@author: Eduardo Pagotto
'''

from typing import Any, Tuple
import redis

class Empty(Exception):
    'Exception raised by Queue.get(block=False)/get_nowait().'
    pass

class RedisQueue(object):
    def __init__(self, redis : redis.Redis, queue_name: str) -> None:
        self.__redis = redis
        self.__queue_name = queue_name

    def qsize(self) -> int:
        return self.__redis.llen(self.__queue_name)

    def empty(self) -> bool:
        return self.qsize() == 0

    def enqueue(self, item: Any) -> int:
        return self.__redis.rpush(self.__queue_name, item)

    def nack(self, item : Any) -> int:
        return self.__redis.lpush(self.__queue_name, item)

    def dequeue(self) -> Any:
        return self.__redis.lpop(self.__queue_name)

    def bdequeue(self, timeout : float = 0) -> Tuple | None:
        return self.__redis.blpop(self.__queue_name, timeout=timeout)

    def get_name(self) -> str:
        return self.__queue_name