'''
Created on 20221110
Update on 20221110
@author: Eduardo Pagotto
'''

import logging
import pathlib
from threading import Lock
from typing import Any, List, Tuple

from tinydb import TinyDB

from SSC.server import splitNamespace, splitTopic
from SSC.server.Cocoon import Cocoon
from SSC.subsys.LockDB import LockDB

class EnttCrt(object):
    def __init__(self, colection_name : str, database : TinyDB, path_storage : str) -> None:

        self.colection_name = colection_name
        self.database = database
        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger('SSC.crt')
        self.lock_func = Lock()
        self.list_entt : List[Cocoon] = []

    def summario(self) ->dict:
        
        result = []
        tot = 0
        with self.lock_func:
            tot = len(self.list_entt)  
            for i in self.list_entt:
                result.append(i.sumary())  

        return {'online': tot, 'tasks': result}

    def stop_func_all(self):

        with self.lock_func:
            for source in self.list_entt:
                source.stop()

            for source in self.list_entt:
                source.join()
                self.list_entt.remove(source)

    def pause_resume(self, func_name : str, is_pause : bool):
        tenant, namespace, name = splitTopic(func_name)   
        with self.lock_func:
            for fun in self.list_entt:
                if ((fun.document['tenant'] == tenant) and (fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
                    msg : str = ''
                    if is_pause:
                        msg = f'{name} paused'
                        fun.pause()
                    else:
                        msg = f'{name} resumed'
                        fun.resume()

                    self.log.info(msg)
                    return msg

        raise Exception(f'{func_name} does not exist')

    def delete(self, func_name : str):

        self.log.debug(f'delete {func_name}')

        tenant, namespace, name = splitTopic(func_name)
        funcValid = None

        with self.lock_func:
            for source in self.list_entt:
                if (tenant == source.document['tenant']) and (namespace == source.document['namespace']) and (name == source.document['name']):
                    self.list_entt.remove(source)
                    funcValid = source

        if funcValid:            
            funcValid.stop()
            funcValid.join()
            with LockDB(self.database, self.colection_name, True) as table:
                table.remove(doc_ids=[funcValid.document.doc_id])

            return f'success delete {func_name}'

        raise Exception(f'{func_name} does not exist')

    def list_all(self, tenant_ns : str) -> List[str]:
        with LockDB(self.database, self.colection_name, False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            tenant, namespace = splitNamespace(tenant_ns)
            if (tenant == item['tenant']) and (namespace == item['namespace']): 
                lista.append(item['name'])

        return lista

    def execute(self) -> Tuple[int, int]:

        tot_proc = 0
        tot_erro = 0
        with self.lock_func:
            for source in self.list_entt:
                ok, erro = source.count_tot()
                tot_proc += ok
                tot_erro += erro

        return tot_proc, tot_erro