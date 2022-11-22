'''
Created on 20221114
Update on 20221120
@author: Eduardo Pagotto
'''

import json
from typing import Any
from tinydb import TinyDB
from tinydb.table import Document
from SSC.Message import Message
from SSC.Sink import Sink
from SSC.subsys.LockDB import LockDB

class SinkTinydb(Sink):
    def __init__(self) -> None:
        super().__init__()

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.config = doc['config']['configs']

        self.database =  TinyDB(doc['storage'] + '/' +self.config['file'])
        return self.config['delay']

    def process(self, content : Message) -> None:
    
        properties = content.properties()
        table_name = properties['table'] if 'table' in properties else 'Default'
        with LockDB(self.database, table_name, True) as table:
            if self.config['fullmsg']:
                table.insert(content.to_dict())
            else:
                try:
                    table.insert(json.loads(content.data()))
                except:
                    table.insert( {'payload': str(content.data())})
