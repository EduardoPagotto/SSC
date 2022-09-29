
from queue import Queue, Empty
from typing import Any, Dict, Optional

class NamedQueue():
    def __init__(self):
        self.map_queues : Dict[str, Queue] = {}

    def exist(self, name) -> bool:
        return name in self.map_queues

    def create(self, name : str):

        if name in self.map_queues:
            raise Exception('Queue ja existe {0}'.format(name))

        nova : Queue = Queue()
        self.map_queues[name] = nova

    def destroy(self, name):
        try:
            del self.map_queues[name]
        except:
            raise Exception('Queue não existe {0}'.format(name))

    def push(self, name: str, value : Any) -> None:
        try:
            o_queue = self.map_queues[name]
            o_queue.put(value)
        except:
            raise Exception('Queue não existe {0}'.format(name))

    def pop(self, name : str) -> Optional[Any]:

        o_queue : Optional[Queue] = None
        try:
            o_queue = self.map_queues[name]
        except:
            raise Exception('Queue não existe {0}'.format(name))

        try:
            val = o_queue.get_nowait()
            return val
        except Empty:
            pass

        return None

    def qsize(self, name : str) -> int:
        try:
            o_queue = self.map_queues[name]
            return o_queue.qsize()
        except:
            raise Exception('Queue não existe {0}'.format(name))

    def empty(self, name : str) -> bool:
        try:
            o_queue = self.map_queues[name]
            return o_queue.empty()
        except:
            raise Exception('Queue não existe {0}'.format(name))


