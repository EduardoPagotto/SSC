from abc import ABC, abstractmethod
import logging
import pathlib

class Function(ABC):
    def __init__(self) -> None:
        self.name : str = ''
        #self.path : pathlib.Path = None
        self.qIn : int = -1
        self.qOut : int = -1
        self.useConfig : dict = {}
        self.log = logging.getLogger('SSC.function')


    @abstractmethod
    def process(self, input : str, context : dict):
        pass

    