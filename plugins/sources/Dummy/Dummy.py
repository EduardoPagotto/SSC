'''
Created on 20221109
Update on 20221109
@author: Eduardo Pagotto
'''

import logging
from typing import Optional

from tinydb.table import Document

from SSC.Source import Source
from SSC.server import EstatData
from SSC.topic.QueueProdCons import QueueProducer


class Dummy(Source):
    def __init__(self) -> None:
        print('Dummy Constructor')
        self.document : Document = Document({}, 0)
        self.count = 0
        self.serial = 0
        self.water_mark = 2
        self.log = logging.getLogger('Dummy')
        super().__init__()

    def start(self, doc : Document) -> int:
        self.document = doc
        try:
            return self.document['config']['cfg']['delay']
        except:
            pass

        return 5

    def process(self, producer : QueueProducer, estat : EstatData) -> bool:

        if producer.size() > self.water_mark:
            return False

        self.count += 1
        if self.count % 2 == 0:
            self.serial += 1
            payload = f'msg {self.serial}'
            self.log.debug(payload)

            producer.send(payload, properties={}, msg_key='', sequence_id=self.serial)
            estat.tot_ok += 1
            return True

        return False

