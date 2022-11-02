'''
Created on 20221101
Update on 20221101
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
from threading import Thread
import time

from typing import Any, List, Optional

from tinydb import TinyDB
from tinydb.table import Document
from SSC.server import create_queue, create_queues

from SSC.topic.Connection import Connection
from SSC.Context import Context
from SSC.topic import Consumer, Producer
from SSC.Function import Function

class FuncCocoon(object):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        self.log = logging.getLogger('SSC.function')
        self.name = params['name']
        self.database : TinyDB = database

        self.consumer : Optional[Consumer] = None
        self.producer : Optional[Producer] = None
        self.document : Document = Document({},0)
        self.tot_proc : int = 0
        self.tot_erro : int = 0
        self.alive : bool = True
        self.paralel : Optional[Thread] = None

        if ('inputs' in params) and (params['inputs'] is not None):
            data_in = create_queues(self.database ,params['inputs'])
            conn_in = Connection(data_in['urlRedis'])
            self.consumer = conn_in.create_consumer(data_in['queue'])

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            conn_out = Connection(data_out['urlRedis'])
            self.producer = conn_out.create_producer(data_out['queue'])


        self.function : Function = self.__load(pathlib.Path(params['py']), params['classname'])

        
    def __load(self, path_file : pathlib.Path, class_name : str) -> Any:
            klass = None

            plugin = str(path_file.parent).replace('/','.') + '.' + class_name

            self.log.debug(f'function import {plugin}')

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)

            except Exception as ex:
                msg_erro = "Could not enable class %s - %s" % (plugin, str(ex))
                self.log.error(msg_erro)
                raise Exception(msg_erro)

            if klass is None:
                self.log.error(f"Could not enable at least one class: {plugin}")
                raise Exception(f"Could not enable at least one class: {plugin}") 

            return klass()

    def execute(self, timeout : int):
        
        self.log.info(f'function thread start... {self.name}')

        if timeout <= 0:
            timeout = 5

        extra_map_puplish : dict[str, Producer] = {}

        while (self.alive):

            inputs = 0
            outputs = 0

            if self.consumer:

                try:

                    res = self.consumer.receive(timeout)
                    if res:
                        for k, v in res.items():
                            #self.log.debug(f'Function exec {self.name} topic in: {self.topic_in.name} ..')
                            inputs += 1
                            self.tot_proc += 1

                            try:
                                ret = self.function.process(v, Context(extra_map_puplish, self.document, k, self.database ,self.log))
                                if (self.producer) and (ret != None):

                                    outputs += 1

                                    #self.log.debug(f'Function exec {self.name} topic out: {self.topic_out.name} ..')
                                    self.producer.send(ret)

                            except Exception as exp:
                                
                                # auto nack
                                #self.topic_in.push(res)
                                #TODO: imlementar erro critico de queue 

                                self.tot_erro += 1
                                self.log.error(f'Function exec {self.name} erro: ' + exp.args[0])
                                time.sleep(1)

                            continue

                except Exception as exp:
                    self.log.error(exp.args[0])
                    self.tot_erro += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(timeout)

        self.log.info(f'function thread stop... {self.name}')