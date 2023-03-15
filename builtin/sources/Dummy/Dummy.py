'''
Created on 20221109
Update on 20230314
@author: Eduardo Pagotto
'''

import logging
from typing import Any

from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context
from SSC.server.QueueProdCons import QueueProducer


class Dummy(Function):
    def __init__(self) -> None:
        print('Dummy Constructor')
        self.document : Document = Document({}, 0)
        self.count = 0
        self.serial = 0
        self.water_mark = 2
        self.log = logging.getLogger('Dummy')
        super().__init__()

    def process(self, input : str, context : Context) -> Any:
        
        producer : QueueProducer = context.get_producer('default')

        if producer.size() <= self.water_mark:

            self.count += 1
            if self.count % 2 == 0:
                self.serial += 1
                payload = f'msg {self.serial}'
                self.log.debug(payload)

                producer.send(payload, {}, '', self.count)

                return 1
        
        return 0

