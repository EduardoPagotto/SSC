'''
Created on 20221019
Update on 20221019
@author: Eduardo Pagotto
'''

from typing import Any, Tuple, Union
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

    def dequeue(self) -> Union[Tuple, Any]:
        result = self.__redis.lpop(self.__queue_name)
        if result is None:
            raise Empty

        return result

    def bdequeue(self, timeout : float = 0) -> Union[Tuple, Any]:
        result = self.__redis.blpop(self.__queue_name, timeout=timeout)
        if result is None:
            raise Empty

        return result

    def get_name(self) -> str:
        return self.__queue_name