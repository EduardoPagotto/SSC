'''
Created on 20221006
Update on 20221006
@author: Eduardo Pagotto
'''

from queue import Queue, Empty
from typing import Any, Optional

class Topic(object):
    def __init__(self, id : int, name: str) -> None:
        self.name : str = name
        self.id : int = id
        self.queue : Queue = Queue()

    def push(self, value : Any) -> None:
         self.queue.put(value)

    def pop(self, timeout: int) -> Optional[Any]:
        try:
            if timeout == 0:
                return self.queue.get_nowait()
            else:
                return self.queue.get(block=True, timeout=timeout)
        except Empty:
            pass

        return None

    def qsize(self) -> int:
        return self.queue.qsize()

    def empty(self) -> bool:
        return self.queue.empty()