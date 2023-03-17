'''
Created on 20230315
Update on 20230315
@author: Eduardo Pagotto
'''

import json
from typing import Optional

import redis

from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context

from SSC.subsys.RedisQueue import RedisQueue

class SinkRedisQueue(Function):
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