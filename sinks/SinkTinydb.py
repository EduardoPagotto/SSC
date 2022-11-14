'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

from tinydb import TinyDB
from tinydb.table import Document
from SSC.Sink import Sink
from SSC.subsys.LockDB import LockDB

class SinkTinydb(Sink):
    def __init__(self) -> None:
        super().__init__()

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.database =  TinyDB(doc['storage'] + '/' +doc['config']['file'])

        return doc['config']['sinktinydb']['delay']

    def process(self, content : dict, topic : str) -> None:
        
        if 'table' in content['properties']:
            with LockDB(self.database, content['properties']['table'], True) as table:
                table.insert(content['payload'])
        else:
            with LockDB(self.database, 'default', True) as table:
                table.insert(content['payload'])
