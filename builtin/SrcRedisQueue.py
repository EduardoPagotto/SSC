'''
Created on 20230315
Update on 20230315
@author: Eduardo Pagotto
'''

import logging
import redis
from queue import Empty

from typing import Optional
from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context

from SSC.subsys.RedisQueue import RedisQueue
from SSC.Message import Generate

class SrcRedisQueue(Function):
    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.ready = False
        #self.queue = RedisQueue(redis.Redis.from_url(url), queue_name)
        self.queue : Optional[RedisQueue] = None
        self.timeout : float = 5.0

    def start(self, params : Document):
        self.config = params['config']
        self.timeout = params['timeout']
        self.ready = True
        self.queue = RedisQueue(redis.Redis.from_url(self.config['url']), self.config['queue_src'])


    def process(self, input : str, context : Context) -> int:
        
        if not self.ready:
            self.start(context.params)

        if context.get_producer_size(context.get_output_queue()) <= self.config['water_mark']:

            if self.queue:

                val = self.queue.bdequeue(self.timeout)
                if val:
                    payload = val[1].decode('utf8')

                    context.publish(context.get_output_queue(), payload, {'queue_src' : self.config['queue_src']}, 'from_redis', Generate.get_id())
                    return 1
                else:
                    raise Empty

            else:
                raise Exception('Sem queue origem')
        
        return 0
    

class DstRedisQueue(Function):
    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.ready = False
        self.queue : Optional[RedisQueue] = None

    def start(self, params : Document):
        self.config = params['config']
        self.ready = True
        self.queue = RedisQueue(redis.Redis.from_url(self.config['url']), self.config['queue_dst'])

    def process(self, input : str, context : Context) -> int:
        
        if not self.ready:
            self.start(context.params)

        if self.queue:
            self.queue.enqueue(input)
            return 1        

        raise Exception('Sem queue destino')