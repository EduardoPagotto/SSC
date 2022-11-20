'''
Created on 20221114
Update on 20221120
@author: Eduardo Pagotto
'''

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
        self.config = doc['config']['sinkcsv']

        self.file =  doc['storage'] + '/' +self.config['file']
        return self.config['delay']

    def process(self, content : Message ) -> None:
        
        #datetime.today().strftime('%Y%m%d')

        payload =json.loads(content.data())
        my_dict = payload['dados']

        path = Path(self.file)
        if path.is_file():
            with open(self.file, 'a') as f:
                w = csv.writer(f)
                w.writerow(my_dict.values())
        else:
            with open(self.file, 'w') as f:
                w = csv.writer(f)
                w.writerow(my_dict.keys())
                w.writerow(my_dict.values()) 
