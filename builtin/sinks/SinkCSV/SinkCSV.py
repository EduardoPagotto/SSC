'''
Created on 20221114
Update on 20221123
@author: Eduardo Pagotto
'''

from datetime import datetime
from pathlib import Path
import csv
import json
from typing import Any
from tinydb.table import Document
from SSC.Message import Message
from SSC.Sink import Sink

class SinkCSV(Sink):
    def __init__(self) -> None:
        super().__init__()

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.config = doc['config']['configs']
        self.field = self.config['field'] if 'field' in self.config else 'field'
        self.file_prefix =  self.doc['storage'] + '/' + self.config['file_prefix'] if 'file_prefix' in self.config else 'file'
        return self.config['delay']

    def process(self, content : Message ) -> None:
        
        file_name = self.file_prefix + '_' + datetime.today().strftime('%Y%m%d') + '.csv'

        payload =json.loads(content.data())

        data : dict = {}
        final : dict = {}
        if self.field:
            if self.field not in payload:
                raise Exception(f'Campo da dados {self.field} nao existe no payload')

            data = payload[self.field]
        else:
            data = payload

        if type(data) == list:
            final = {}
            for linha in data:
                field_name = ''
                for k, v in linha.items():
                    if k == 'field':
                        field_name = v
                    elif k == 'value':
                        final[field_name] = v
                    else:
                        raise Exception('Dado formatado de form incorreta missing("field/value")')

        path = Path(file_name)
        if path.is_file():
            with open(file_name, 'a') as f:
                w = csv.writer(f)
                w.writerow(final.values())
        else:
            with open(file_name, 'w') as f:
                w = csv.writer(f)
                w.writerow(final.keys())
                w.writerow(final.values()) 
