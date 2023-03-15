'''
Created on 20221114
Update on 20230315
@author: Eduardo Pagotto
'''

import csv
import json

from datetime import datetime
from pathlib import Path
from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context

class SinkCSV(Function):
    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.field : str = ''
        self.file_prefix : str = ''
        self.ready = False

    def start(self, params : Document):
        self.config = params['config']
        self.field = self.config['field'] if 'field' in self.config else 'field'
        self.file_prefix =  params['storage'] + '/' + self.config['file_prefix'] if 'file_prefix' in self.config else 'file'
        self.ready = True

    def process(self, input : str, context : Context) -> int:
        
        if not self.ready:
            self.start(context.params)

        file_name = self.file_prefix + '_' + datetime.today().strftime('%Y%m%d') + '.csv'

        payload =json.loads(input)

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

        return 1