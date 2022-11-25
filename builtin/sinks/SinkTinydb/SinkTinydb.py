'''
Created on 20221114
Update on 20221123
@author: Eduardo Pagotto
'''

from datetime import datetime
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
        self.file_prefix =  self.doc['storage'] + '/' + self.config['file_prefix'] if 'file_prefix' in self.config else 'file'
        return self.config['delay']

    def process(self, content : Message) -> None:
    
        file_name = self.file_prefix + '_' + datetime.today().strftime('%Y%m%d') + '.json'
        database =  TinyDB(file_name)

        properties = content.properties()
        table_name = properties['table'] if 'table' in properties else 'Default'
        with LockDB(database, table_name, True) as table:
            if self.config['fullmsg']:
                table.insert(content.to_dict())
            else:
                try:
                    table.insert(json.loads(content.data()))
                except:
                    table.insert( {'payload': str(content.data())})
