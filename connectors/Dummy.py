'''
Created on 20221109
Update on 20221109
@author: Eduardo Pagotto
'''

from typing import Optional

from SSC.Connector import Connector
from SSC.Message import Message

class Dummy(Connector):
    def __init__(self) -> None:
        print('Dummy Constructor')
        super().__init__()

    def start(self, config : dict) -> None:
        pass

    def process(self, config : dict) -> Optional[Message]:
        return None