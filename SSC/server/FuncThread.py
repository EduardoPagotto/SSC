'''
Created on 20221102
Update on 20221104
@author: Eduardo Pagotto
'''

import importlib
import json
import logging
import pathlib
import threading
import time

from typing import Any, Optional

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server import create_queue, create_queues
from SSC.Context import Context
from SSC.Function import Function
from SSC.topic.QueueProdCons import QueueConsumer, QueueProducer

class FuncData(object):
    def __init__(self) -> None:
        self.tot_ok = 0
        self.tot_err = 0
        self.pause = False
        self.done = False

    def summary(self):
        return {'ok' : self.tot_ok, 'err': self.tot_err, 'pause':str(self.pause)}


class FuncThread(threading.Thread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        self.esta = FuncData()

        self.timeout = 5 # TODO: parame
        temp_name : str = f't_{index}_' + params['name']

        self.consumer : Optional[QueueConsumer] = None
        self.producer : Optional[QueueProducer] = None

        self.log = logging.getLogger('SSC.FuncThread')
        self.database : TinyDB = database
        self.document = params

        if ('inputs' in params) and (params['inputs'] is not None):
            data_in = create_queues(self.database ,params['inputs'])
            self.consumer = QueueConsumer(data_in['urlRedis'], data_in['queue'])

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            self.producer = QueueProducer(data_in['urlRedis'], data_out['queue'], temp_name)

        self.function : Function = self.__load(pathlib.Path(params['py']), params['classname'])

        super().__init__(None, None, temp_name)

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

    def run(self):

        self.log.info(f'started {self.name}')

        if self.timeout <= 0:
            self.timeout = 5

        extra_map_puplish : dict[str, QueueProducer] = {}

        is_running = True

        while (not self.esta.done):

            inputs = 0
            outputs = 0

            if self.esta.pause is True:

                if is_running is True:
                    self.log.info(f'pause {self.name}')
                    is_running = False

                time.sleep(self.timeout)
                continue
            else:
                if is_running is False:
                    self.log.info(f'resume {self.name}')
                    is_running = True

            if self.consumer:

                try:

                    res = self.consumer.receive(self.timeout)
                    if res:
                        for k, v in res.items():
                            #self.log.debug(f'Function exec {self.name} topic in: {self.topic_in.name} ..')
                            inputs += 1
                            self.esta.tot_ok += 1
                            content = json.loads(v)

                            try:
                                ret = self.function.process(content['payload'], Context(content, extra_map_puplish, self.document, k, self.database, self.log))
                                if (self.producer) and (ret != None):

                                    outputs += 1

                                    #self.log.debug(f'Function exec {self.name} topic out: {self.topic_out.name} ..')
                                    self.producer.send(ret)

                            except Exception as exp:
                                
                                # auto nack
                                #self.topic_in.push(res)
                                #TODO: imlementar erro critico de queue 

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