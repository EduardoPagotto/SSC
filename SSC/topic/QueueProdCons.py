'''
Created on 20221019
Update on 20221019
@author: Eduardo Pagotto
'''

from typing import Any, List
import redis

from SSC.topic import Producer, Consumer
from SSC.topic.RedisQueue import RedisQueue

class QueueProducer(Producer):
    def __init__(self, redis : redis.Redis, queue_name : str) -> None:
        self.queue = RedisQueue(redis, queue_name)

    def send(self, register : Any) -> int:
        return self.queue.enqueue(register)

class QueueConsumer(Consumer):
    def __init__(self, redis : redis.Redis, queues_name : List[str]) -> None:
        self.queues : List[RedisQueue] = []
        for item in queues_name:
            self.queues.append(RedisQueue(redis, item))

    def receive(self, timeout : float = 0) -> dict[str, Any] | None:

        output : dict[str, Any] = {}
        for item in self.queues:
            if timeout == 0:
                val = item.dequeue()
                if val:
                    output[val[0]] = val[1]
            else:
                val = item.bdequeue(timeout)
                if val:
                    output[val[0]] = val[1]


        return output


    def nack(self, queue_name : str, register : Any) -> int:
        for item in self.queues:
            if item.get_name() == queue_name:
                return item.nack(register)

        return -1