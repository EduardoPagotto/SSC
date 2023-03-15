'''
Created on 20221111
Update on 20230314
@author: Eduardo Pagotto
'''

import yaml
import json
import logging
import pathlib
import time
from typing import Any, List, Optional
from configparser import ConfigParser
from tinydb.table import Document

from SSC.Function import Function
from SSC.Context import Context
from SSC.server.QueueProdCons import QueueProducer

class Watchdogdir(Function):
    def __init__(self) -> None:
        print('Watchdogdir Constructor')
        self.serial : int = 0
        self.log = logging.getLogger('Watchdogdir')
        self.input = pathlib.Path('./input')
        self.output : Optional[pathlib.Path] = None
        self.erro : Optional[pathlib.Path] = None
        self.pattern : List[str] = ['txt', 'json', 'yaml']
        self.watermark : int = 2
        self.peeding_file = None
        self.ready : bool = False

        super().__init__()

    def start(self, params : Document):
        
        self.log.info(f'Watchdogdir {params["name"]}') 

        if 'configs' in params['config']: 
            if 'input' in params['config']['configs']:
                self.input = pathlib.Path(params['storage'] + '/' + params['config']['configs']['input'])

            if 'output' in params['config']['configs']:
                self.output = pathlib.Path(params['storage'] + '/' + params['config']['configs']['output'])
                self.output.mkdir(parents=True, exist_ok=True)
                self.log.info(f'output :{self.output.resolve()}')

            if 'erro' in params['config']['configs']:
                self.erro = pathlib.Path(params['storage'] + '/' + params['config']['configs']['erro'])
                self.erro.mkdir(parents=True, exist_ok=True)
                self.log.info(f'erro :{self.erro.resolve()}')

            if 'pattern' in params['config']['configs']:
                self.pattern = params['config']['configs']['pattern']

            if 'watermark' in params['config']['configs']:
                self.watermark = params['config']['configs']['watermark']

            if 'delay' in params['config']['configs']:
                self.delay = params['config']['configs']['delay']

        self.input.mkdir(parents=True, exist_ok=True)

        self.log.info(f'input :{self.input.resolve()}') 
        self.log.info(f'pattern :{str(self.pattern)}')
        self.log.info(f'watermark :{str(self.watermark)}')
        self.log.info(f'delay :{str(self.delay)}')

        self.ready = True

    def exec_error(self, producer : QueueProducer, item : pathlib.Path):

        if self.erro:

            dst = str(self.erro.resolve()) + '/' + item.name
            item.replace(dst)
            properties = {'valid': False, 'file': item.name, 'src': str(item.resolve()), 'dst': dst}
            producer.send('', properties=properties, msg_key=dst, sequence_id=self.serial)

        else:

            item.unlink()
            properties = {'valid': False, 'file': item.name, 'src': str(item.resolve())}
            producer.send('', properties=properties, msg_key='', sequence_id=self.serial)  

        self.log.warn(f'parse fail {item.name}')

    def exec_ok(self, payload : str, producer : QueueProducer, item : pathlib.Path):

        if self.output:

            dst = str(self.output.resolve()) + '/' + item.name
            item.replace(dst)
            properties = {'valid': True, 'file': item.name, 'src': str(item.resolve()), 'dst': dst}
            producer.send(payload, properties=properties, msg_key=dst, sequence_id=self.serial)

        else:

            item.unlink()
            properties = {'valid': True, 'file': item.name, 'src': str(item.resolve())}
            producer.send(payload, properties=properties, msg_key='', sequence_id=self.serial)

        self.log.info(f'parse ok {item.name}')

    
    def process(self, input : str, context : Context) -> Any:

        if not self.ready:
            self.start(context.params)

        producer : QueueProducer = context.get_producer('default')

        if producer.size() >= self.watermark:
            return 0

        lista_arquivos : List[pathlib.Path] = []
        count = 0
        for x in self.input.iterdir():

            if count < 2:
                time.sleep(1)

            if x.is_file():
                lista_arquivos.append(x)
                if len(lista_arquivos) >= self.watermark:
                    break

            
        for item in lista_arquivos:
        
            ext = item.suffix.lower()
            payload : str = ''

            if ext == '.json': # 

                try:
                    self.exec_ok(json.dumps(json.loads(item.read_text())), producer, item)

                except Exception as exp:
                    #self.log.debug(f'Json fail {item.resolve()} err: {exp.args[0]}')
                    self.exec_error(producer, item)

            elif ext == '.yaml':

                try:
                    self.exec_ok(json.dumps(yaml.safe_load(item.read_text())), producer, item)

                except Exception as exp:
                    #self.log.debug(f'YAML fail {item.resolve()} err: {exp.args[0]}')
                    self.exec_error(producer, item)

            elif ext == '.txt' or ext == '.ini':
                
                try:
                    parser = ConfigParser()
                    parser.read(item.resolve())
                    self.exec_ok(json.dumps({section: dict(parser.items(section)) for section in parser.sections()}), producer, item)

                except Exception as exp:
                    #self.log.debug(f'INI fail {item.resolve()} err: {exp.args[0]}')
                    self.exec_error(producer, item)

            else:
                self.exec_error(producer, item)
                
        return len(lista_arquivos)

