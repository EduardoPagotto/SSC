'''
Created on 20221108
Update on 20221123
@author: Eduardo Pagotto
'''

import pathlib
import time

from tinydb.table import Document
from SSC.Source import Source

#from SSC.server import create_queue
from SSC.server.Namespace import Namespace
from SSC.server.EntThread import EntThread
from SSC.server.QueueProdCons import QueueProducer

class SourceThread(EntThread):
    def __init__(self, index : int, params : Document, namespace : Namespace) -> None:

        super().__init__('src',index, params)
        self.ns = namespace
        self.producer = QueueProducer(params['output'], namespace.queue_get(params['output']), params['name'])
        self.source : Source = self.load(pathlib.Path(params['archive']), params['classname'])

    def run(self):
        self.log.info(f'started {self.name}')
        self.timeout = self.source.start(self.document)
        if self.timeout <= 0:
            self.timeout = 5

        while (not self.esta.done):

            if self.is_paused():
                time.sleep(self.timeout)
                continue

            try:
                if self.source.process(self.producer, self.esta):
                    continue

            except Exception as exp:
                self.log.error(str(exp.args))
                self.esta.tot_err += 1

            time.sleep(1)

        self.log.info(f'stopped {self.name}')