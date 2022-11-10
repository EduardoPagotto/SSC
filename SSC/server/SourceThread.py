'''
Created on 20221108
Update on 20221109
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
import threading
import time

from typing import Any, Optional

from tinydb import TinyDB
from tinydb.table import Document
from SSC.Source import Source

from SSC.server import EstatData, create_queue
from SSC.topic.QueueProdCons import QueueProducer

class SourceThread(threading.Thread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        self.esta = EstatData()
        self.timeout = 5 # TODO: parame
        self.producer : Optional[QueueProducer] = None

        self.log = logging.getLogger('SSC.SrcThread')
        self.database : TinyDB = database
        self.document = params

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            self.producer = QueueProducer(data_out['urlRedis'], data_out['queue'], params['name'])

        self.source : Source = self.__load(pathlib.Path(params['archive']), params['classname'])

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
        is_running = True
        self.timeout = self.source.start(self.document)
        if self.timeout <= 0:
            self.timeout = 5

        while (not self.esta.done):

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
                data = self.source.process(self.producer.size())
                if data:
                    self.producer.send(data.payload, data.properties, data.msg_key, data.sequence_id)
                    self.esta.tot_ok += 1
                    continue

            except Exception as exp:
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            time.sleep(self.timeout)

        self.log.info(f'stopped {self.name}')