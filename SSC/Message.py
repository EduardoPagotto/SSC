'''
Created on 20221103
Update on 20221103
@author: Eduardo Pagotto
'''

from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
import threading

class Generate:
    __sequence_id : int = 0
    __mutex_id = threading.Lock()

    @staticmethod
    def get_id() -> int:
        with Generate.__mutex_id:
            Generate.__sequence_id += 1
            return Generate.__sequence_id

    @staticmethod
    def get_ts() -> float:
        return datetime.now(tz=timezone.utc).timestamp()

@dataclass(frozen=True, kw_only=True, slots=True)
class Message:
    seq_id : int
    payload : str
    topic : str
    event_time : float = 0
    producer : str = field(default_factory=str)
    key : str = field(default_factory=str)
    properties : dict = field(default_factory=dict)
    schemma : dict = field(default_factory=dict)
    msg_id : int = field(init=False, default_factory = Generate.get_id)
    push_time : float = field(init=False, default_factory = Generate.get_ts)


    # def __post_init__(self) -> None:
    #     self.event_time = Generate.get_id()
