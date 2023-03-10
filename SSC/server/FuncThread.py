'''
Created on 20221102
Update on 20221123
@author: Eduardo Pagotto
'''

import pathlib
import time

from typing import Optional

from tinydb import TinyDB
from tinydb.table import Document

#from SSC.server import create_queue, create_queues
from SSC.Context import Context
from SSC.Function import Function
from SSC.server.EntThread import EntThread
from SSC.topic.QueueProdCons import QueueConsumer, QueueProducer
from SSC.topic.RedisQueue import Empty

class FuncThread(EntThread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        super().__init__('func',index, params, database)

        data_in = create_queues(self.database ,params['inputs'])  # FIXME: retornar string do mapa de queues
        self.consumer : QueueConsumer = QueueConsumer(data_in['urlRedis'], data_in['queue'])
        self.producer : Optional[QueueProducer] = None

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])  # FIXME: retornar string do mapa de queues
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
                content = self.consumer.receive(self.timeout)
                inputs += 1
                self.esta.tot_ok += 1

                try:
                    ret = self.function.process(content.data(), Context(content, extra_map_puplish, self.document, self.database, self.log))
                    if (self.producer) and (ret != None):
                        outputs += 1
                        self.producer.send(ret)

                except Exception as exp:   
                    self.esta.tot_err += 1
                    self.log.error(f'Function exec {self.name} erro: ' + exp.args[0])
                    time.sleep(1)

                continue

            except Empty:
                pass

            except Exception as exp:
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(1)

        self.log.info(f'stopped {self.name}')