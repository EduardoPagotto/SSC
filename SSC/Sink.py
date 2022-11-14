'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod

from tinydb.table import Document

class Sink(ABC):

    @abstractmethod
    def start(self, doc : Document) -> int:
        pass

    @abstractmethod
    def process(self, content : dict, topic : str) -> None:
        pass
