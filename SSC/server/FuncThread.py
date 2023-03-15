'''
Created on 20221102
Update on 20230314
@author: Eduardo Pagotto
'''

import pathlib
import time

from typing import Optional

from tinydb.table import Document
from queue import Empty

from SSC.server.Namespace import Namespace
from SSC.Context import Context
from SSC.Message import Message
from SSC.Function import Function
from SSC.server.EntThread import EntThread
from SSC.server.QueueProdCons import QueueConsumer, QueueProducer

class FuncThread(EntThread):
    def __init__(self, sufix : str, index : int, params : Document, namespace : Namespace) -> None:

        super().__init__(sufix, index, params)

        self.ns : Namespace = namespace

        self.map_producer : dict[str, QueueProducer] = {}

        self.consumer : Optional[QueueConsumer] = None
        if ('inputs' in params) and (params['inputs'] is not None):
            self.consumer = QueueConsumer(namespace.queues_get('inputs', params))

        if ('output' in params) and (params['output'] is not None):
            q = QueueProducer(params['output'], namespace.queue_get(params['output']), params['name'])
            self.map_producer[params['output']] = q
            self.map_producer['default'] =  q

        self.function : Function = self.load(pathlib.Path(params['py']), params['classname'])

    def run(self):

        seq_id = 0
        self.log.info(f'{self.name} started')

        while (not self.esta.done):

            if self.is_paused():
                if self.sleep_read == 0:
                    time.sleep(5.0)
                else:
                    time.sleep(self.sleep_read)
                    
                continue

            try:
                if self.consumer: # sink e function

                    content = self.consumer.receive(self.sleep_read) 

                    ret = self.function.process(content.data(), Context(content, self.map_producer, self.params, self.ns, self.log))
                    if ('default' in self.map_producer) and ret is not None:
                        self.map_producer['default'].send(content=ret, properties=content.properties(), msg_key=content.partition_key(), sequence_id=content.seq_id())
                        self.esta.tot_ok += 1

                else: # exclusivo Source

                    content = Message.create(seq_id = seq_id, payload = '', queue = '', properties = {}, producer = self.params['name'], key = '')
    
                    ret = self.function.process(content.data(), Context(content, self.map_producer, self.params, self.ns, self.log))

                    if ret > 0:
                        self.esta.tot_ok += ret
                    else:
                        if self.sleep_read > 0.0:
                            time.sleep(self.sleep_read)
                        else:
                            time.sleep(5.0)

            except Empty:
                if self.sleep_read == 0.0:
                    time.sleep(5.0)    

            except Exception as exp:   
                self.esta.tot_err += 1
                self.log.error(f'{self.name} exec erro: ' + exp.args[0])
                time.sleep(5.0)

        self.log.info(f'{self.name} stopped')