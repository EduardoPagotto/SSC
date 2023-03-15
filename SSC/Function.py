'''
Created on 20220924
Update on 20230314
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from typing import Any
from SSC.Context import Context

class Function(ABC):
    @abstractmethod
    def process(self, input : str, context : Context) -> Any:
        pass
