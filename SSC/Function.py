from abc import ABC, abstractmethod
import pathlib

class Function(ABC):
    def __init__(self) -> None:
        self.name : str = ''
        #self.path : pathlib.Path = None
        self.qIn : str = ''
        self.qOut : str = ''
        self.useConfig : dict = {}


    @abstractmethod
    def process(self, input : str, context):
        pass

    