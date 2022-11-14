'''
Created on 20221111
Update on 20221114
@author: Eduardo Pagotto
'''


import yaml
import json
import logging
import pathlib
import time
from typing import List, Optional
from configparser import ConfigParser
from tinydb.table import Document

from SSC.Source import Source
from SSC.server import EstatData
from SSC.topic.QueueProdCons import QueueProducer

class Watchdogdir(Source):
    def __init__(self) -> None:
        print('Watchdogdir Constructor')
        self.document : Document = Document({}, 0)
        self.serial : int = 0
        self.log = logging.getLogger('Watchdogdir')
        self.input = pathlib.Path('./input')
        self.work : Optional[pathlib.Path] = None
        self.erro : Optional[pathlib.Path] = None
        self.pattern : List[str] = ['txt', 'json', 'yaml']
        self.watermark : int = 2
        self.delay = 10
        self.peeding_file = None

        super().__init__()

    def start(self, doc : Document) -> int:
        self.document = doc

        self.log.info(f'Watchdogdir {doc["name"]}') 

        if 'watchdogdir' in self.document['config']: 
            if 'input' in self.document['config']['watchdogdir']:
                self.input = pathlib.Path(self.document['storage'] + '/' + self.document['config']['watchdogdir']['input'])

            if 'work' in self.document['config']['watchdogdir']:
                self.work = pathlib.Path(self.document['storage'] + '/' + self.document['config']['watchdogdir']['work'])
                self.work.mkdir(parents=True, exist_ok=True)
                self.log.info(f'work :{self.work.resolve()}')

            if 'erro' in self.document['config']['watchdogdir']:
                self.erro = pathlib.Path(self.document['storage'] + '/' + self.document['config']['watchdogdir']['erro'])
                self.erro.mkdir(parents=True, exist_ok=True)
                self.log.info(f'erro :{self.erro.resolve()}')

            if 'pattern' in self.document['config']['watchdogdir']:
                self.pattern = self.document['config']['watchdogdir']['pattern']

            if 'watermark' in self.document['config']['watchdogdir']:
                self.watermark = self.document['config']['watchdogdir']['watermark']

            if 'delay' in self.document['config']['watchdogdir']:
                self.delay = self.document['config']['watchdogdir']['delay']

        self.input.mkdir(parents=True, exist_ok=True)

        self.log.info(f'input :{self.input.resolve()}') 
        self.log.info(f'pattern :{str(self.pattern)}')
        self.log.info(f'watermark :{str(self.watermark)}')
        self.log.info(f'delay :{str(self.delay)}')

        return self.delay

    def erro_parser(self, item : pathlib.Path):
        if self.erro:
            dst = str(self.erro.resolve()) + '/' + item.name
            item.replace(dst)

    def process(self, producer : QueueProducer, estat : EstatData) -> bool:

        if producer.size() >= self.watermark:
            return False

        lista_arquivos : List[pathlib.Path] = []
        for x in self.input.iterdir():
            if x.is_file():
                lista_arquivos.append(x)
                time.sleep(1)
                if len(lista_arquivos) >= self.watermark:
                    break

            
        for item in lista_arquivos:
        
            ext = item.suffix.lower()
            payload : str = ''

            if ext == '.json': # 

                try:
                    payload = json.dumps(json.loads(item.read_text())) 
                except Exception as exp:
                    self.log.error(f'Json fail {item.resolve()} err: {exp.args[0]}')
                    self.erro_parser(item)
                    continue

            elif ext == '.yaml':

                try:
                    payload = json.dumps(yaml.safe_load(item.read_text()))
                except Exception as exp:
                    self.log.error(f'YAML fail {item.resolve()} err: {exp.args[0]}')
                    self.erro_parser(item)
                    continue


            elif ext == '.txt' or ext == '.ini':
                
                try:
                    parser = ConfigParser()
                    parser.read(item.resolve())
                    payload = json.dumps({section: dict(parser.items(section)) for section in parser.sections()})

                except Exception as exp:
                    self.log.error(f'INI fail {item.resolve()} err: {exp.args[0]}')
                    self.erro_parser(item)
                    continue             

            else:
                continue
                
            dst = ''
            if self.work:
                dst = str(self.work.resolve()) + '/' + item.name
                item.replace(dst)
            else:
                item.unlink()    

            self.log.info(f'process file {item.name} path: {dst}')
            properties = {'name': item.name, 'sufix': item.suffix, 'path': dst}
            
            producer.send(payload, properties=properties, msg_key=dst, sequence_id=self.serial)
            estat.tot_ok += 1

        if len(lista_arquivos) > 0:
            return True

        return False
