'''
Created on 20221108
Update on 20221109
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

from tinydb.table import Document

from SSC.Message import Message

@dataclass(frozen=True, kw_only=True, slots=True)
class SourceData:
    payload : Any
    sequence_id : int
    msg_key : str 
    properties : dict

class Source(ABC):

    @abstractmethod
    def start(self, doc : Document) -> int:
        pass

    @abstractmethod
    def process(self, size: int) -> Optional[SourceData]:
        pass
