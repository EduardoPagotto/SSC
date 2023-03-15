'''
Created on 20221109
Update on 20230315
@author: Eduardo Pagotto
'''

import logging
from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context

class Dummy(Function):
    def __init__(self) -> None:
        print('Dummy Constructor')
        self.document : Document = Document({}, 0)
        self.count = 0
        self.serial = 0
        self.water_mark = 2
        self.log = logging.getLogger('Dummy')
        super().__init__()

    def process(self, input : str, context : Context) -> int:
        
        if context.get_producer_size(context.get_output_queue()) <= self.water_mark:

            self.count += 1
            if self.count % 2 == 0:
                self.serial += 1
                payload = f'msg {self.serial}'
                self.log.debug(payload)

                context.publish(context.get_output_queue(), payload, {}, '', self.count)
                return 1
        
        return 0

