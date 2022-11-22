'''
Created on 20221121
Update on 20221121
@author: Eduardo Pagotto
'''

import logging
import pathlib
from tinydb.table import Document

from SSC.Message import Message
from SSC.Sink import Sink

from SSF.ClientSSF import ClientSSF

class SinkWriterFiles(Sink):
    def __init__(self) -> None:
        super().__init__()
        self.output = pathlib.Path('./output')
        self.erro = pathlib.Path('./erro')
        self.others = pathlib.Path('./others')
        self.delay = 5
        self.watermark = 5
        self.ssf : ClientSSF | None = None
        self.log = logging.getLogger('SinkWriterFiles')

    def start(self, doc : Document) -> int:
        self.doc = doc
        self.config = doc['config']['configs']

        self.log.info(f'{doc["name"]} start ')

        self.output = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['output'])
        self.output.mkdir(parents=True, exist_ok=True)

        self.erro = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['erro'])
        self.erro.mkdir(parents=True, exist_ok=True)

        self.others = pathlib.Path(self.doc['storage'] + '/' + self.doc['config']['configs']['others'])
        self.others.mkdir(parents=True, exist_ok=True)

        self.delay = self.config['delay'] if 'delay' in self.config else 5
        self.watermark = self.config['watermark'] if 'watermark' in self.config else 2

        self.ssf = ClientSSF(self.config['ssf_url']) 

        return self.delay

    def process(self, content : Message ) -> None:
        
        topic_name = content.topic_name()
        prop = content.properties()
        if 'erro' not in topic_name:
            if prop['valid'] is True:
                src = pathlib.Path(prop['dst'])
                dst = str(self.output.resolve()) + '/' +  prop['file']
                src.replace(dst)
                self.log.info(f'{self.doc["name"]} save {dst}')

                for extra in prop['extras']:
                    id = extra['id']
                    if id > 0:
                        if self.ssf:
                            file_prop = self.ssf.info(id)
                            file = pathlib.Path(self.others, file_prop)
                            valid, msg = self.ssf.download(id, str(file.resolve()))
                            if not valid:
                                raise Exception(msg)
                            else:
                                self.log.info(f'{self.doc["name"]} save extra {msg}')

                        else:
                            self.log.error(f'Critico falta cfg de ssf no arguivo de configuracao')
                    else:
                        self.log.error(f'Extra invalido {extra["erro"]}')

        else:
            if prop['valid'] is True:
                # valido mas com erro
                src = pathlib.Path(prop['dst'])
                dst = str(self.erro.resolve()) + '/' +  prop['file']
                src.replace(dst)
                self.log.warning(f'{self.doc["name"]} save fail {dst}')
            else:
                self.log.error(f'{self.doc["name"]} registro invalido {prop["dst"]}')
