'''
Created on 20221019
Update on 20221019
@author: Eduardo Pagotto
'''

from abc import ABC, abstractmethod
from typing import Any

class Producer(ABC):
    @abstractmethod
    def send(self, register : Any) -> int:
        pass

class Consumer(ABC):
    @abstractmethod
    def receive(self, timeout : float = 0) -> dict[str, Any] | None:
        pass

    @abstractmethod
    def nack(self, queue_name : str, register : Any) -> int:
        pass