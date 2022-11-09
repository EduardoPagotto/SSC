'''
Created on 20221108
Update on 20221109
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
from SSC.Connector import Connector

from SSC.server import create_queue
from SSC.topic.QueueProdCons import QueueProducer

class ConnData(object): # FIXME: usar o mesmo de fun mas mudar nome na base!!
    def __init__(self) -> None:
        self.tot_ok = 0
        self.tot_err = 0
        self.pause = False
        self.done = False

    def summary(self):
        return {'ok' : self.tot_ok, 'err': self.tot_err, 'pause':str(self.pause)}


class ConnectorThread(threading.Thread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        self.esta = ConnData()
        self.timeout = 5 # TODO: parame
        self.producer : Optional[QueueProducer] = None

        self.log = logging.getLogger('SSC.ConnThread')
        self.database : TinyDB = database
        self.document = params

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            self.producer = QueueProducer(data_out['urlRedis'], data_out['queue'], params['name'])

        self.connector : Connector = self.__load(pathlib.Path(params['archive']), 'connectors') # FIXME: esta errado!!!!

        super().__init__(None, None, f't_{index}_' + params['name'])

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

        is_running = True

        self.connector.start({}) # FIXME: colocar a catrga do cfg aqui !!!!

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

            try:
                data = self.connector.process({}) # FIXME: dados para criar a msg
                if data:
                    content = json.loads(data)
                    outputs += 1

                    #self.log.debug(f'Function exec {self.name} topic out: {self.topic_out.name} ..')
                    self.producer.send(content)
                    continue

            except Exception as exp:
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            if (inputs == 0) and (outputs == 0):
                time.sleep(self.timeout)

        self.log.info(f'stopped {self.name}')