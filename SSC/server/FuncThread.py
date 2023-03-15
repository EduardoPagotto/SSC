'''
Created on 20221102
Update on 20230315
@author: Eduardo Pagotto
'''

import pathlib
import time

import logging
import importlib
from typing import Any, Optional
import threading
from tinydb.table import Document
from queue import Empty

from SSC.server.Namespace import Namespace
from SSC.Context import Context
from SSC.Message import Message
from SSC.Function import Function
from SSC.server.QueueProdCons import QueueConsumer, QueueProducer

from SSC.server import EstatData

class FuncThread(threading.Thread):
    def __init__(self, sufix : str, index : int, params : Document, namespace : Namespace) -> None:
        
        super().__init__(None, None, f'{sufix}_{index}_' + params['name'])

        self.map_producer : dict[str, QueueProducer] = {}
        self.esta = EstatData()
        self.sufix = sufix
        self.ns : Namespace = namespace
        self.timeout : float = float(params['timeout']) if 'timeout' in params else 5.0 # 5 segundos default
        self.params = params
        self.is_running = True
    
        self.log = logging.getLogger('SSC.EntThread')

        self.consumer : Optional[QueueConsumer] = None
        if 'inputs' in params:
            self.consumer = QueueConsumer(namespace.queues_get('inputs', params))

        if 'output' in params:
            out_val = params['output']
            self.map_producer[out_val] = QueueProducer(out_val, namespace.queue_get(out_val), params['name'])

        self.function : Function = self.load(pathlib.Path(params['py']), params['classname'])

    def run(self):

        seq_id = 0
        self.log.info(f'{self.name} started')

        while (not self.esta.done):

            if self.is_paused():
                time.sleep(self.timeout)    
                continue

            try:
                content = self.consumer.receive(self.timeout) if self.consumer else \
                    Message.create(seq_id = seq_id, payload = '', queue = '', properties = {}, producer = self.params['name'], key = '')

                ret = self.function.process(content.data(), Context(content, self.map_producer, self.params, self.ns, self.log))

                if ret > 0:
                    self.esta.tot_ok += ret
                else:
                    time.sleep(self.timeout)

            except Empty:
                pass

            except Exception as exp:   
                self.esta.tot_err += 1
                self.log.error(f'{self.name} exec erro: ' + exp.args[0])
                time.sleep(1.0)

        self.log.info(f'{self.name} stopped')

    def is_paused(self) -> bool:
        if self.esta.pause is True:

            if self.is_running is True:
                self.log.info(f'{self.name} pause')
                self.is_running = False

            return True
        else:
            if self.is_running is False:
                self.log.info(f'{self.name} resume')
                self.is_running = True

        return False

    def load(self, path_file : pathlib.Path, class_name : str) -> Any:
            klass = None

            plugin = str(path_file.parent).replace('/','.') + '.' + class_name

            self.log.info(f'{self.name} import {plugin}')

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)

            except Exception as ex:
                msg_erro = f"{self.name} could not enable class %s - %s" % (plugin, str(ex))
                self.log.error(msg_erro)
                raise Exception(msg_erro)

            if klass is None:
                self.log.error(f"{self.name} could not enable at least one class: {plugin}")
                raise Exception(f"{self.name} could not enable at least one class: {plugin}") 

            return klass()   