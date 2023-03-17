'''
Created on 20221111
Update on 20230317
@author: Eduardo Pagotto
'''

import yaml
import json
import csv
import logging
import pathlib
import time
from pathlib import Path
from datetime import datetime
from typing import Any, List, Optional
from configparser import ConfigParser

from tinydb.table import Document
from tinydb import TinyDB

from SSF.ClientSSF import ClientSSF

from SSC.Function import Function
from SSC.Context import Context
from SSC.subsys.LockDB import LockDB

class SrcWatchdogdir(Function):
    """
        Classe que faz pooling em diretorio aguardando arquivos (json, yaml, ini) 
    """

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

        if 'input' in params['config']:
            self.input = pathlib.Path(params['storage'] + '/' + params['config']['input'])

        if 'output' in params['config']:
            self.output = pathlib.Path(params['storage'] + '/' + params['config']['output'])
            self.output.mkdir(parents=True, exist_ok=True)
            self.log.info(f'output :{self.output.resolve()}')

        if 'erro' in params['config']:
            self.erro = pathlib.Path(params['storage'] + '/' + params['config']['erro'])
            self.erro.mkdir(parents=True, exist_ok=True)
            self.log.info(f'erro :{self.erro.resolve()}')

        if 'pattern' in params['config']:
            self.pattern = params['config']['pattern']

        if 'watermark' in params['config']:
            self.watermark = params['config']['watermark']

        self.input.mkdir(parents=True, exist_ok=True)

        self.log.info(f'input :{self.input.resolve()}') 
        self.log.info(f'pattern :{str(self.pattern)}')
        self.log.info(f'watermark :{str(self.watermark)}')

        self.ready = True

    def exec_error(self, context :Context , item : pathlib.Path):

        if self.erro:
            dst = str(self.erro.resolve()) + '/' + item.name
            item.replace(dst)
            properties = {'valid': False, 'file': item.name, 'src': str(item.resolve()), 'dst': dst}
            context.publish(context.get_output_queue(), '', properties=properties, msg_key=dst, sequence_id=self.serial)

        else:
            item.unlink()
            properties = {'valid': False, 'file': item.name, 'src': str(item.resolve())}
            context.publish(context.get_output_queue(), '', properties=properties, msg_key='', sequence_id=self.serial)  

        self.log.warn(f'parse fail {item.name}')

    def exec_ok(self, payload : str, context : Context, item : pathlib.Path):

        try:     
            if self.output:
                dst = str(self.output.resolve()) + '/' + item.name
                item.replace(dst)
                properties = {'valid': True, 'file': item.name, 'src': str(item.resolve()), 'dst': dst}
                context.publish(context.get_output_queue(), payload, properties=properties, msg_key=dst, sequence_id=self.serial)

            else:
                item.unlink()
                properties = {'valid': True, 'file': item.name, 'src': str(item.resolve())}
                context.publish(context.get_output_queue(), payload, properties=properties, msg_key='', sequence_id=self.serial)

            self.log.info(f'parse ok {item.name}')

        except Exception as exp:
            self.log.debug(f'fail {item.resolve()} err: {exp.args[0]}')
            self.exec_error(context, item)
    
    def process(self, input : str, context : Context) -> Any:

        if not self.ready:
            self.start(context.params)

        if context.get_producer_size(context.get_output_queue()) >= self.watermark:
            return 0

        lista_arquivos : List[pathlib.Path] = []

        for x in self.input.iterdir():

            if len(lista_arquivos) < 2:
                time.sleep(1)

            if x.is_file():
                lista_arquivos.append(x)
                if len(lista_arquivos) >= self.watermark:
                    break

            
        for item in lista_arquivos:
        
            ext = item.suffix.lower()
            payload : str = ''

            if ext == '.json': # 
                self.exec_ok(json.dumps(json.loads(item.read_text())), context, item)

            elif ext == '.yaml':
                self.exec_ok(json.dumps(yaml.safe_load(item.read_text())), context, item)

            elif ext == '.txt' or ext == '.ini':
                try:
                    parser = ConfigParser()
                    parser.read(item.resolve())
                    self.exec_ok(json.dumps({section: dict(parser.items(section)) for section in parser.sections()}), context, item)

                except Exception as exp:
                    self.exec_error(context, item)

            else:
                self.exec_error(context, item)
                
        return len(lista_arquivos)

# -------------------------------------------------

class DstCSV(Function):
    """
        Classe que cria e adiciona dados em arquivo csv
    """

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
        self.spliter_file = self.config['spliter_file'] if 'spliter_file' in self.config else None 
        self.ready = True

    def log_erro_arquivo(self, prop):

        file_name = self.file_prefix + '_erro_' + datetime.today().strftime('%Y%m%d') + '.csv'
        final = {'arquivo' : prop['file']}

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

    def process(self, input : str, context : Context) -> int:
        
        if not self.ready:
            self.start(context.params)

        file_name = self.file_prefix + '_' + datetime.today().strftime('%Y%m%d') + '.csv'

        prop = context.get_message_properties()
        if 'valid' in prop:
            if prop['valid'] == False:
                self.log_erro_arquivo(prop)
                return 1

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
                    
        elif type(data) == dict:
            final = data
            if self.spliter_file:
                if self.spliter_file in final:
                    file_name = self.file_prefix + '_' + final[self.spliter_file]  + '_' + datetime.today().strftime('%Y%m%d') + '.csv'

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
    
# -------------------------------------------------

class DstWriterFiles(Function):
    """
        Classe que move arquivo de diretorio e copia arquivo extra vindo do SSF 
    """

    def __init__(self) -> None:
        super().__init__()
        self.output = pathlib.Path('./output')
        self.erro = pathlib.Path('./erro')
        self.others = pathlib.Path('./others')
        self.watermark = 5
        self.ssf : ClientSSF | None = None
        self.ready : bool = False
        self.log = logging.getLogger('DstWriterFiles')

    def start(self, params : Document):

        self.log.info(f'{params["name"]} start ')

        self.config = params['config']

        self.output = pathlib.Path(params['storage'] + '/' + params['config']['output'])
        self.output.mkdir(parents=True, exist_ok=True)

        self.erro = pathlib.Path(params['storage'] + '/' + params['config']['erro'])
        self.erro.mkdir(parents=True, exist_ok=True)

        self.others = pathlib.Path(params['storage'] + '/' + params['config']['others'])
        self.others.mkdir(parents=True, exist_ok=True)

        self.watermark = self.config['watermark'] if 'watermark' in self.config else 2

        self.ssf = ClientSSF(self.config['ssf_url']) 

        self.ready = True

    def process(self, input : str, context : Context) -> int:

        if not self.ready:
            self.start(context.params)

        queue_name = context.get_current_message_queue_name() #content.queue_name()
        prop = context.get_message_properties() #.properties()
        if 'erro' not in queue_name:
            if prop['valid'] is True:
                src = pathlib.Path(prop['dst'])
                dst = str(self.output.resolve()) + '/' +  prop['file']
                src.replace(dst)
                self.log.info(f'{context.get_function_name()} save {dst}')

                for extra in prop['extras']:
                    id = extra['id']
                    if id > 0:
                        if self.ssf:
                            file_prop = self.ssf.info(id)

                            nf = pathlib.Path(file_prop['pathfile'])

                            file = pathlib.Path(self.others, nf.name)
                            valid, msg = self.ssf.download(id, str(file.resolve()))
                            if not valid:
                                raise Exception(msg)
                            else:
                                self.log.info(f'{context.get_function_name()} save extra {msg}')

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
                self.log.warning(f'{context.get_function_name()} save fail {dst}')
            else:
                self.log.error(f'{context.get_function_name()} registro invalido {prop["dst"]}')

        return 1
    
# -------------------------------------------------

class DstTinydb(Function):
    """
        Classe que envia dados para aquivo json (TynyDB)
    """

    def __init__(self) -> None:
        super().__init__()
        self.config : dict = {}
        self.file_prefix = ''
        self.ready = False

    def start(self, params : Document):
        self.config = params['config']
        self.file_prefix =  params['storage'] + '/' + self.config['file_prefix'] if 'file_prefix' in self.config else 'file'
        self.ready = True
        
    def process(self, input : str, context : Context) -> int:

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

        return 1