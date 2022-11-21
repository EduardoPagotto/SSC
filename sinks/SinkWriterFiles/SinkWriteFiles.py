'''
Created on 20221121
Update on 20221121
@author: Eduardo Pagotto
'''

import pathlib
import json
from tinydb.table import Document

from SSC.Message import Message
from SSC.Sink import Sink

class SinkWriterFiles(Sink):
    def __init__(self) -> None:
        super().__init__()
        self.output = pathlib.Path('./output')
        self.erro = pathlib.Path('./erro')
        self.others = pathlib.Path('./others')
        self.delay = 5
        self.watermark = 5

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.config = doc['config']['configs']

        self.output = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['output'])
        self.output.mkdir(parents=True, exist_ok=True)

        self.erro = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['erro'])
        self.erro.mkdir(parents=True, exist_ok=True)

        self.others = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['others'])
        self.others.mkdir(parents=True, exist_ok=True)

        self.delay = self.config['delay'] if 'delay' in self.config else 5
        self.watermark = self.config['watermark'] if 'watermark' in self.config else 2

        return self.delay

    def process(self, content : Message ) -> None:
        
        #datetime.today().strftime('%Y%m%d')

        payload =json.loads(content.data())
        my_dict = payload['dados']

        # path = Path(self.file)
        # if path.is_file():
        #     with open(self.file, 'a') as f:
        #         w = csv.writer(f)
        #         w.writerow(my_dict.values())
        # else:
        #     with open(self.file, 'w') as f:
        #         w = csv.writer(f)
        #         w.writerow(my_dict.keys())
        #         w.writerow(my_dict.values()) 

    