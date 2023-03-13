'''
Created on 20221114
Update on 20230313
@author: Eduardo Pagotto
'''

import pathlib
import time

from tinydb.table import Document

from SSC.server.Namespace import Namespace
from SSC.Sink import Sink
from SSC.server.EntThread import EntThread
from SSC.server.QueueProdCons import QueueConsumer
from queue import Empty
  
class SinkThread(EntThread):
    def __init__(self, index : int, params : Document,  namespace : Namespace) -> None:

        super().__init__('sinks',index, params)

        self.consumer = QueueConsumer(namespace.queues_get('inputs', params))
        self.sink : Sink = self.load(pathlib.Path(params['archive']), params['classname'])

    def run(self):

        self.log.info(f'started {self.name}')

        self.timeout = self.sink.start(self.document)
        if self.timeout <= 0:
            self.timeout = 5

        while (not self.esta.done):
            try:            
                if self.is_paused():
                    time.sleep(self.timeout)
                    continue

                self.sink.process(self.consumer.receive(self.timeout))
                self.esta.tot_ok += 1

            except Empty:
                time.sleep(1)

            except Exception as exp:
                self.esta.tot_err += 1
                self.log.error(f'Sink {self.name} erro: ' + str(exp))
                time.sleep(1)

        self.log.info(f'stopped {self.name}')