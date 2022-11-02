'''
Created on 20221101
Update on 20221102
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
import threading
import time

from typing import Any, List, Optional, Tuple

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server import create_queue, create_queues
from SSC.subsys.LockDB import LockDB
from SSC.topic.Connection import Connection
from SSC.Context import Context
from SSC.topic import Consumer, Producer
from SSC.Function import Function

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

        self.consumer : Optional[Consumer] = None
        self.producer : Optional[Producer] = None

        self.log = logging.getLogger('SSC.FuncThread')
        self.database : TinyDB = database
        self.document = params

        if ('inputs' in params) and (params['inputs'] is not None):
            data_in = create_queues(self.database ,params['inputs'])
            conn_in = Connection(data_in['urlRedis'])
            self.consumer = conn_in.create_consumer(data_in['queue'])

        if ('output' in params) and (params['output'] is not None):
            data_out = create_queue(self.database, params['output'])
            conn_out = Connection(data_out['urlRedis'])
            self.producer = conn_out.create_producer(data_out['queue'])

        self.function : Function = self.__load(pathlib.Path(params['py']), params['classname'])

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

        extra_map_puplish : dict[str, Producer] = {}

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


class FuncCocoon(object):
    def __init__(self, params : Document | dict, database : TinyDB) -> None:

        self.log = logging.getLogger('SSC.function')
        self.name = params['name']
        self.database : TinyDB = database

        self.list_t : List[FuncThread] = []

        if type(params) == dict:
            with LockDB(self.database, 'funcs', True) as table:
                self.document = table.get(doc_id=table.insert(params))
        else:
            self.document = params

        try:
            for c in range(0, params['parallelism']):
                th = FuncThread(c, self.document, self.database)
                self.list_t.append(th)
        except Exception as exp:
            self.log.critical(f'start func error {exp.args[0]}')
            with LockDB(self.database, 'funcs', True) as table:
                table.remove(doc_ids=[self.document.doc_id])

            raise Exception(exp.args)


    def start(self):
        self.log.debug(f'{self.name} signed to start')
        for t in self.list_t:
            t.start()

    def stop(self):
        self.log.debug(f'{self.name} signed to stop')
        for t in self.list_t:
            t.esta.done = True

    def join(self): 
        # FIXME: implementar um kill depois de um tempo sem resposta
        self.log.debug(f'{self.name} signed to join')
        for t in self.list_t:
            t.join()

    def pause(self):
        self.log.debug(f'{self.name} signed to pause')
        for t in self.list_t:
            t.esta.pause = True

    def resume(self):
        self.log.debug(f'{self.name} signed to resume')
        for t in self.list_t:
            t.esta.pause = False

    def sumary(self) -> dict:

        lista = []
        tot_ok = 0
        tot_err = 0
        for t in self.list_t:
            lista.append({'thread': t.name , 'estat': t.esta.summary()})
            tot_ok += t.esta.tot_ok
            tot_err += t.esta.tot_err

        return {'name':self.name, 'ok': tot_ok, 'err': tot_err,'threads': lista}

    def count_tot(self) -> Tuple[int, int]:

        tot_ok = 0
        tot_err = 0
        for t in self.list_t:
            tot_ok += t.esta.tot_ok
            tot_err += t.esta.tot_err

        return tot_ok, tot_err
