'''
Created on 20221110
Update on 20230314
@author: Eduardo Pagotto
'''

import importlib
import logging
import pathlib
import threading
from typing import Any

from tinydb.table import Document

from SSC.server import EstatData

class EntThread(threading.Thread):
    def __init__(self, sufix : str, index : int, params : Document) -> None:
        self.esta = EstatData()
        self.sufix = sufix
        self.sleep_read : float = float(params['sleep_read']) if 'sleep_read' in params else 0.0 # 5 segundos default
        self.params = params
        self.is_running = True
        self.log = logging.getLogger('SSC.EntThread')

        super().__init__(None, None, f'{sufix}_{index}_' + params['name'])

    def is_paused(self) -> bool:
        if self.esta.pause is True:

            if self.is_running is True:
                self.log.info(f'{self.name} pause')
                self.is_running = False

            return True
        else:
            if self.is_running is False:
                self.log.info(f'{self.name} resume')
                self.is_running = True

        return False

    def load(self, path_file : pathlib.Path, class_name : str) -> Any:
            klass = None

            plugin = str(path_file.parent).replace('/','.') + '.' + class_name

            self.log.info(f'{self.name} import {plugin}')

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)

            except Exception as ex:
                msg_erro = f"{self.name} could not enable class %s - %s" % (plugin, str(ex))
                self.log.error(msg_erro)
                raise Exception(msg_erro)

            if klass is None:
                self.log.error(f"{self.name} could not enable at least one class: {plugin}")
                raise Exception(f"{self.name} could not enable at least one class: {plugin}") 

            return klass()   