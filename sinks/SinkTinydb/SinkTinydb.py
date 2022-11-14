'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

from typing import Any
from tinydb import TinyDB
from tinydb.table import Document
from SSC.Sink import Sink
from SSC.subsys.LockDB import LockDB

class SinkTinydb(Sink):
    def __init__(self) -> None:
        super().__init__()

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.database =  TinyDB(doc['storage'] + '/' +doc['config']['sinktinydb']['file'])

        return doc['config']['sinktinydb']['delay']

    def process(self, content : Any , topic : str) -> None:
        
        table_name = content['properties']['table'] if 'table' in content['properties'] else 'Default'

        payload = content['payload'] if type(content['payload']) == dict else {'payload': str(content['payload'])}

        with LockDB(self.database, table_name, True) as table:
            table.insert(payload)

