'''
Created on 20221108
Update on 20221108
@author: Eduardo Pagotto
'''

import logging
import pathlib
from threading import Lock
from typing import List, Tuple

from tinydb import TinyDB

from SSC.server import splitNamespace, splitTopic
from SSC.server.ConnectorCocoon import ConnectorCocoon
from SSC.subsys.LockDB import LockDB

class ConnectorCrt(object):

    def __init__(self, database : TinyDB, path_storage : str) -> None: # FIXME?: TUDO  A FAZER!!!!

        self.database = database
        self.storage = pathlib.Path(path_storage, 'tenant')
        self.storage.mkdir(parents=True, exist_ok=True)

        self.lock_func = Lock()
        self.list_function : List[ConnectorCocoon] = []

        self.log = logging.getLogger('SSC.ConnectorCrt')

        self.load_funcs_db()

    def summario(self) ->dict:
        
        result = []
        tot = 0
        with self.lock_func:
            tot = len(self.list_function)  
            for i in self.list_function:
                result.append(i.sumary())  

        return {'online': tot, 'tasks': result}

    def create(self, params : dict) -> str:

        self.log.debug(f"function create {params['name']}")
        with self.lock_func:
            for func in self.list_function:
                if (params['tenant'] == func.document['tenant']) and (params['namespace'] == func.document['namespace']) and (params['name'] == func.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

            cocoon : ConnectorCocoon = ConnectorCocoon(params, self.database)
            cocoon.start()
            self.list_function.append(cocoon)

        return f"success create {params['name']}"

    def stop_func_all(self):

        with self.lock_func:
            for func in self.list_function:
                func.stop()

            for func in self.list_function:
                func.join()
                self.list_function.remove(func)

    def pause_resume(self, func_name : str, is_pause : bool):
        tenant, namespace, name = splitTopic(func_name)   
        with self.lock_func:
            for fun in self.list_function:
                if ((fun.document['tenant'] == tenant) and (fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
                    msg : str = ''
                    if is_pause:
                        msg = f'func {name} paused'
                        fun.pause()
                    else:
                        msg = f'func {name} resumed'
                        fun.resume()

                    self.log.info(msg)
                    return msg

        raise Exception(f'function {func_name} does not exist')

    def delete(self, func_name : str):

        self.log.debug(f'function delete {func_name}')

        tenant, namespace, name = splitTopic(func_name)
        funcValid = None

        with self.lock_func:
            for func in self.list_function:
                if (tenant == func.document['tenant']) and (namespace == func.document['namespace']) and (name == func.document['name']):
                    self.list_function.remove(func)
                    funcValid = func

        if funcValid:            
            funcValid.stop()
            funcValid.join()
            with LockDB(self.database, 'funcs', True) as table:
                table.remove(doc_ids=[funcValid.document.doc_id])

            return f'success delete {func_name}'

        raise Exception(f'function {func_name} does not exist')

    def list_all(self, tenant_ns : str) -> List[str]:
        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            tenant, namespace = splitNamespace(tenant_ns)
            if (tenant == item['tenant']) and (namespace == item['namespace']): 
                lista.append(item['name'])

        return lista

    def load_funcs_db(self):

        with LockDB(self.database, 'funcs', False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                with self.lock_func:
                    self.log.debug(f'function load from db: {params["name"]}')
                    cocoon : ConnectorCocoon = ConnectorCocoon(params, self.database)
                    cocoon.start()
                    self.list_function.append(cocoon)
            except:
                pass


    def execute(self) -> Tuple[int, int]:

        tot_proc = 0
        tot_erro = 0
        with self.lock_func:
            for func in self.list_function:
                ok, erro = func.count_tot()
                tot_proc += ok
                tot_erro += erro

        return tot_proc, tot_erro
