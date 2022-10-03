'''
Created on 20220924
Update on 20221003
@author: Eduardo Pagotto
'''


from abc import ABC, abstractmethod
import logging
import pathlib

class Function(ABC):
    def __init__(self) -> None:
        self.id = -1
        self.name : str = ''
        #self.path : pathlib.Path = None
        self.qIn : int = -1
        self.qOut : int = -1
        self.useConfig : dict = {}
        self.log = logging.getLogger('SSC.function')


    @abstractmethod
    def process(self, input : str, context : dict):
        pass

    