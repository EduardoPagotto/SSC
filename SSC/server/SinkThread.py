'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

import json
import pathlib
import time

from tinydb import TinyDB
from tinydb.table import Document

from SSC.server import create_queues
from SSC.Sink import Sink
from SSC.server.EntThread import EntThread
from SSC.topic.QueueProdCons import QueueConsumer

  
class SinkThread(EntThread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        super().__init__('sinks',index, params, database)

        data_in = create_queues(self.database ,params['inputs'])
        self.consumer = QueueConsumer(data_in['urlRedis'], data_in['queue'])
        self.sink : Sink = self.load(pathlib.Path(params['py']), params['classname'])

    def run(self):

        self.log.info(f'started {self.name}')

        self.timeout = self.sink.start(self.document)
        if self.timeout <= 0:
            self.timeout = 5

        while (not self.esta.done):
            
            outputs = 0

            if self.is_paused():
                time.sleep(self.timeout)
                continue

            try:
                res = self.consumer.receive(self.timeout)
                if res:
                    for k, v in res.items():
                        self.esta.tot_ok += 1
                        content = json.loads(v)

                        try:
                            self.sink.process(content, k)
                            self.esta.tot_ok += 1
                            outputs += 1

                        except Exception as exp:
                            
                            self.esta.tot_err += 1
                            self.log.error(f'Sink {self.name} erro: ' + exp.args[0])
                            time.sleep(1)

                        continue

            except Exception as exp:
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            if (outputs == 0):
                time.sleep(self.timeout)

        self.log.info(f'stopped {self.name}')