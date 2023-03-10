'''
Created on 20221019
Update on 20230310
@author: Eduardo Pagotto
'''

import json

from typing import Any, Dict, List

from SSC.Message import Message

from queue import Queue, Empty

class QueueProducer(object):
    #def __init__(self, url : str, queue_name : str, producer_name : str) -> None:
    def __init__(self, queue_name : str, queue : Queue, producer_name : str) -> None:
        self.queue_name = queue_name
        self.producer_name = producer_name
        self.queue = queue #RedisQueue(redis.Redis.from_url(url), queue_name)

    def send(self, content : Any, properties : dict  = {}, msg_key : str = '' ,sequence_id : int = 0) -> None:

        msg = Message.create(seq_id = sequence_id,
                                payload = content,
                                topic = self.queue_name,
                                properties = properties,
                                producer = self.producer_name,
                                key = msg_key)



        self.queue.put(json.dumps(msg.to_dict()))

    def size(self) -> int:
        return self.queue.qsize()

class QueueConsumer(object):
    def __init__(self, map_queues : Dict[str, Queue]) -> None:
        self.pending : Queue = Queue()
        self.map : Dict[str, Queue] = map_queues

    def receive(self, timeout : float = 0) -> Message:

        if self.pending.qsize() > 0:
            return self.pending.get()

        for _, queue in self.map.items():

            if timeout == 0:
                val = queue.get_nowait()
                if val:
                    self.pending.put(Message.from_dic(json.loads(val.decode('utf8'))))
                    
            else:
                try:
                    val = queue.get(block=True, timeout=timeout) 
                    if val:
                        self.pending.put(Message.from_dic(json.loads(val[1].decode('utf8'))))
                except Empty:
                    continue

        if self.pending.qsize() > 0:
            return self.pending.get()

        raise Empty


    def nack(self, queue_name : str, register : Any) -> int:
        for queue_name_local, queue in self.map.items():
            if queue_name_local == queue_name:
                queue.put(register) #item.nack(register)
                return 0
            
        return -1

