'''
Created on 20221103
Update on 20230313
@author: Eduardo Pagotto
'''

from datetime import datetime, timezone
#from dataclasses import dataclass, field
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

# @dataclass(frozen=True, kw_only=True, slots=True)
# class Message:
#     seq_id : int
#     payload : str
#     queue : str
#     producer : str = field(default_factory=str)
#     key : str = field(default_factory=str)
#     properties : dict = field(default_factory=dict)
#     schemma : dict = field(default_factory=dict)
#     msg_id : int = field(init=False, default_factory = Generate.get_id)
#     push_time : float = field(init=False, default_factory = Generate.get_ts)
#     event_timestamp : float = 0
#     redelivery : int = 0

#    # def __post_init__(self) -> None:
#    #     self.event_time = Generate.get_id()

class Message(object):
    def __init__(self) -> None:
        
        self._seq_id : int = 0
        self._payload : str = ''
        self._queue : str = ''
        self._producer : str = ''
        self._key : str = ''
        self._properties : dict = {}
        self._schemma : str = ''
        self._msg_id : int = 0
        self._push_time : float = 0.0
        self._event_timestamp : float = 0.0
        self._redelivery : int = 0

    @staticmethod
    def create(seq_id : int, payload : str, queue : str, properties : dict = {}, producer : str = '', key : str = ''):

        msg = Message()

        msg._seq_id = seq_id
        msg._payload = payload
        msg._queue = queue
        msg._producer = producer
        msg._key = key
        msg._properties = properties
        msg._schemma = ''
        msg._msg_id = Generate.get_id()
        msg._push_time = Generate.get_ts()
        msg._event_timestamp = 0.0
        msg._redelivery = 0

        return msg

    
    @staticmethod
    def from_dic(data : dict):
        msg = Message()

        msg._seq_id = data['seq_id']
        msg._payload = data['payload']
        msg._queue = data['queue']
        msg._producer = data['producer']
        msg._key = data['key']
        msg._properties = data['properties']
        msg._schemma = data['schemma']
        msg._msg_id = data['msg_id']
        msg._push_time = data['push_time']
        msg._event_timestamp = Generate.get_ts()
        # FIXME: 
        if 'redelivery' in data: 
            msg._redelivery = data['redelivery']
        else:
            msg._redelivery = 0

        return msg

    def seq_id(self) -> int:
        return self._seq_id

    def data(self) -> str:
        return self._payload

    def properties(self) -> dict:
        return self._properties

    def partition_key(self) -> str:
        return self._key

    def publish_timestamp(self) -> float:
        return self._push_time

    def event_timestamp(self) -> float:
        return self._event_timestamp

    def message_id(self) -> int:
        return self._msg_id

    def queue_name(self) -> str:
        return self._queue

    def redelivery_count(self) -> int:
        return self._redelivery

    def schema_version(self) -> str:
        return self._schemma

    def to_dict(self) -> dict:
        
        return {'seq_id' : self._seq_id,
                'payload' : self._payload,
                'queue' : self._queue,
                'producer' : self._producer,
                'key' : self._key, 
                'properties' : self._properties,
                'schemma' : self._schemma,
                'msg_id' : self._msg_id,
                'push_time' : self._push_time, 
                'event_timestamp' : self._event_timestamp,
                'redelivery' : self._redelivery}
        

