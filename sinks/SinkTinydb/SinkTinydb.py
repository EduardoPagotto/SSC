'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

import json
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
        self.config = doc['config']['sinktinydb']

        self.database =  TinyDB(doc['storage'] + '/' +self.config['file'])
        return self.config['delay']

    def process(self, content : Any , topic : str) -> None:
        
        table_name = content['properties']['table'] if 'table' in content['properties'] else 'Default'

        with LockDB(self.database, table_name, True) as table: # TODO: colocar conversao json no peyload por parametro no config ??
            if self.config['fullmsg']:
                table.insert(content)
            else:
                try:
                    table.insert(json.loads(content['payload']))
                except:
                    table.insert( {'payload': str(content['payload'])})

