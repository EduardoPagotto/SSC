'''
Created on 20221108
Update on 20221111
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod

from tinydb.table import Document

from SSC.server import EstatData
from SSC.server.QueueProdCons import QueueProducer

class Source(ABC):

    @abstractmethod
    def start(self, doc : Document) -> int:
        pass

    @abstractmethod
    def process(self, producer : QueueProducer, estat : EstatData) -> bool:
        pass
