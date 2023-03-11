'''
Created on 20221102
Update on 20230310
@author: Eduardo Pagotto
'''

import pathlib
import time

from typing import Optional

from tinydb.table import Document
from queue import Empty

from SSC.server.Namespace import Namespace
from SSC.Context import Context
from SSC.Function import Function
from SSC.server.EntThread import EntThread
from SSC.server.QueueProdCons import QueueConsumer, QueueProducer

class FuncThread(EntThread):
    def __init__(self, index : int, params : Document, namespace : Namespace) -> None:

        super().__init__('func',index, params)

        self.ns = namespace

        novo : dict = {}
        if type(params['inputs']) == list: 
            for item in params['inputs']:
                novo[item] = namespace.queue_get(item)

        elif type(params['inputs']) == str:
           novo[params['inputs']] = namespace.queue_get(params['inputs'])
           
        else:
            raise Exception('queue name invalid ' + str(params['inputs']))

        self.consumer = QueueConsumer(novo)

        self.producer : Optional[QueueProducer] = None
        if ('output' in params) and (params['output'] is not None):
            self.producer = QueueProducer(params['output'], namespace.queue_get(params['output']), params['name'])

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

                try: # FIXME: Context tem que ser refeito!!!!!
                    ret = self.function.process(content.data(), Context(content, extra_map_puplish, self.document, self.ns, self.log))
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