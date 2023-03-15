'''
Created on 20221114
Update on 20230314
@author: Eduardo Pagotto
'''

from datetime import datetime
import json
from typing import Any
from tinydb import TinyDB
from tinydb.table import Document
from SSC.Message import Message
from SSC.Context import Context
from SSC.Function import Function
from SSC.subsys.LockDB import LockDB

class SinkTinydb(Function):
    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.file_prefix = ''
        self.ready = False

    def start(self, params : Document):
        self.config = params['config']['configs']
        self.file_prefix =  params['storage'] + '/' + self.config['file_prefix'] if 'file_prefix' in self.config else 'file'
        self.ready = True
        
    def process(self, input : str, context : Context) -> None:

        if not self.ready:
            self.start(context.params)

        file_name = self.file_prefix + '_' + datetime.today().strftime('%Y%m%d') + '.json'
        database =  TinyDB(file_name)

        properties = context.get_message_properties()

        table_name = properties['table'] if 'table' in properties else 'Default'
        with LockDB(database, table_name, True) as table:
            if self.config['fullmsg']:
                table.insert(context.msg.to_dict())
            else:
                try:
                    table.insert(json.loads(input))
                except:
                    table.insert( {'payload': str(input)})
