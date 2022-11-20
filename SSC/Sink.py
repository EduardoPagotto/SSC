'''
Created on 20221114
Update on 20221120
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from typing import Any

from tinydb.table import Document
from SSC.Message import Message

class Sink(ABC):

    @abstractmethod
    def start(self, doc : Document) -> int:
        pass

    @abstractmethod
    def process(self, content : Message) -> None:
        pass
