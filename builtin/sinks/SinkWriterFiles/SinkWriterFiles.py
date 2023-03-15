'''
Created on 20221121
Update on 20230314
@author: Eduardo Pagotto
'''

import logging
import pathlib
from tinydb.table import Document

from SSC.Message import Message
from SSC.Function import Function
from SSC.Context import Context

from SSF.ClientSSF import ClientSSF

class SinkWriterFiles(Function):
    def __init__(self) -> None:
        super().__init__()
        self.output = pathlib.Path('./output')
        self.erro = pathlib.Path('./erro')
        self.others = pathlib.Path('./others')
        self.watermark = 5
        self.ssf : ClientSSF | None = None
        self.ready : bool = False
        self.log = logging.getLogger('SinkWriterFiles')

    def start(self, params : Document):

        self.log.info(f'{params["name"]} start ')

        self.config = params['config']['configs']

        self.output = pathlib.Path(params['storage'] + '/' + params['config']['configs']['output'])
        self.output.mkdir(parents=True, exist_ok=True)

        self.erro = pathlib.Path(params['storage'] + '/' + params['config']['configs']['erro'])
        self.erro.mkdir(parents=True, exist_ok=True)

        self.others = pathlib.Path(params['storage'] + '/' + params['config']['configs']['others'])
        self.others.mkdir(parents=True, exist_ok=True)

        self.watermark = self.config['watermark'] if 'watermark' in self.config else 2

        self.ssf = ClientSSF(self.config['ssf_url']) 

        self.ready = True

    # def process(self, content : Message ) -> None:
    def process(self, input : str, context : Context) -> None:

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
