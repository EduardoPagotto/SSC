'''
Created on 20221102
Update on 20221109
@author: Eduardo Pagotto
'''

import json
import pathlib
import time

from typing import Optional

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server import create_queue, create_queues
from SSC.Context import Context
from SSC.Function import Function
from SSC.server.EntThread import EntThread
from SSC.topic.QueueProdCons import QueueConsumer, QueueProducer

  
class FuncThread(EntThread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        super().__init__('func',index, params, database)

        data_in = create_queues(self.database ,params['inputs'])
        self.consumer : QueueConsumer = QueueConsumer(data_in['urlRedis'], data_in['queue'])
        self.producer : Optional[QueueProducer] = None

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            self.producer = QueueProducer(data_out['urlRedis'], data_out['queue'], params['name'])

        self.function : Function = self.load(pathlib.Path(params['py']), params['classname'])

    def run(self):

        self.log.info(f'started {self.name}')

        if self.timeout <= 0:
            self.timeout = 5

        extra_map_puplish : dict[str, QueueProducer] = {}
        while (not self.esta.done):

            inputs = 0
            outputs = 0

            if self.is_paused():
                time.sleep(self.timeout)
                continue

            try:
                res = self.consumer.receive(self.timeout)
                if res:
                    for k, v in res.items():
                        inputs += 1
                        self.esta.tot_ok += 1
                        content = json.loads(v)

                        try:
                            ret = self.function.process(content['payload'], Context(content, extra_map_puplish, self.document, k, self.database, self.log))
                            if (self.producer) and (ret != None):

                                outputs += 1
                                self.producer.send(ret)

                        except Exception as exp:
                            
                            self.esta.tot_err += 1
                            self.log.error(f'Function exec {self.name} erro: ' + exp.args[0])
                            time.sleep(1)

                        continue

            except Exception as exp:
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(self.timeout)

        self.log.info(f'stopped {self.name}')