'''
Created on 20221108
Update on 20221110
@author: Eduardo Pagotto
'''

import pathlib
import time

from tinydb import TinyDB
from tinydb.table import Document
from SSC.Source import Source

from SSC.server import create_queue
from SSC.server.EntThread import EntThread
from SSC.topic.QueueProdCons import QueueProducer

class SourceThread(EntThread):
    def __init__(self, index : int, params : Document, database : TinyDB) -> None:

        super().__init__('src',index, params, database)

        data_out = create_queue(self.database, params['output'])
        self.producer = QueueProducer(data_out['urlRedis'], data_out['queue'], params['name'])
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
                self.log.error(exp.args[0])
                self.esta.tot_err += 1

            time.sleep(self.timeout)

        self.log.info(f'stopped {self.name}')