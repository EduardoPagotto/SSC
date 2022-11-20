'''
Created on 20221019
Update on 20221120
@author: Eduardo Pagotto
'''

import json
import redis

from typing import Any, List

from SSC.topic.RedisQueue import Empty, RedisQueue
from SSC.Message import Message

import queue

class QueueProducer(object):
    def __init__(self, url : str, queue_name : str, producer_name : str) -> None:
        self.topic = queue_name
        self.producer_name = producer_name
        self.queue = RedisQueue(redis.Redis.from_url(url), queue_name)

    def send(self, content : Any, properties : dict  = {}, msg_key : str = '' ,sequence_id : int = 0) -> int:

        msg = Message.create(seq_id=sequence_id,
                                payload=content,
                                topic=self.topic,
                                properties=properties,
                                producer=self.producer_name,
                                key = msg_key)



        return self.queue.enqueue(json.dumps(msg.to_dict()))

    def size(self) -> int:
        return self.queue.qsize()

class QueueConsumer(object):
    def __init__(self, url : str, queues_name : List[str] | str) -> None:

        self.queues : List[RedisQueue] = []

        self.pending : queue.Queue = queue.Queue()

        if type(queues_name) == list:         
            for item in queues_name:
                self.queues.append(RedisQueue(redis.Redis.from_url(url), item))
        elif type(queues_name) == str:
            self.queues.append(RedisQueue(redis.Redis.from_url(url), queues_name))
        else:
            raise Exception('topic name invalid ' + str(queues_name))

    def receive(self, timeout : float = 0) -> Message:

        if self.pending.qsize() > 0:
            return self.pending.get()

        for item in self.queues:
            if timeout == 0:
                val = item.dequeue()
                if val:
                    self.pending.put(Message.from_dic(json.loads(val.decode('utf8'))))
            else:
                val = item.bdequeue(timeout)
                if val:
                    self.pending.put(Message.from_dic(json.loads(val[1].decode('utf8'))))

        if self.pending.qsize() > 0:
            return self.pending.get()

        raise Empty


    def nack(self, queue_name : str, register : Any) -> int:
        for item in self.queues:
            if item.get_name() == queue_name:
                return item.nack(register)

        return -1