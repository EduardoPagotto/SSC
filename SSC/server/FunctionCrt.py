'''
Created on 20221006
Update on 20230313
@author: Eduardo Pagotto
'''

import logging

from threading import Lock
from typing import Any, List, Tuple

from SSC.server.Namespace import Namespace, splitQueue
from SSC.server.FuncCocoon import FuncCocoon
from SSC.subsys.LockDB import LockDB

class FunctionCrt(object):

    def __init__(self, sufix : str, namespace : Namespace) -> None:

        self.colection_name = sufix
        self.ns = namespace
        self.log = logging.getLogger('SSC.EnttCrt')
        self.lock_func = Lock()
        self.list_entt : List[FuncCocoon] = []

        self.load_funcs_db()

    def create(self, params : dict) -> str:

        self.log.debug(f"create {params['name']}")

        params['storage'] = str(self.ns.path_storage)
        with self.lock_func:
            for func in self.list_entt:
                if (params['namespace'] == func.document['namespace']) and (params['name'] == func.document['name']):
                    raise Exception(f'function {params["name"]} already exists')

            cocoon : FuncCocoon = FuncCocoon(self.colection_name, params, self.ns)
            cocoon.start()
            self.list_entt.append(cocoon)

        return f"success create {params['name']}"

    def load_funcs_db(self):

        with LockDB(self.ns.database, self.colection_name, False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                with self.lock_func:
                    self.log.debug(f'load from db: {params["name"]}')
                    cocoon : FuncCocoon = FuncCocoon(self.colection_name, params, self.ns)
                    cocoon.start()
                    self.list_entt.append(cocoon)
            except:
                pass

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
        namespace, name = splitQueue(func_name)   
        with self.lock_func:
            for fun in self.list_entt:
                if ((fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
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

        namespace, name = splitQueue(func_name)
        funcValid = None

        with self.lock_func:
            for source in self.list_entt:
                if (namespace == source.document['namespace']) and (name == source.document['name']):
                    self.list_entt.remove(source)
                    funcValid = source

        if funcValid:            
            funcValid.stop()
            funcValid.join()
            with LockDB(self.ns.database, self.colection_name, True) as table:
                table.remove(doc_ids=[funcValid.document.doc_id])

            return f'success delete {func_name}'

        raise Exception(f'{func_name} does not exist')

    def list_all(self, ns : str) -> List[str]:
        with LockDB(self.ns.database, self.colection_name, False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            if (ns == item['namespace']): 
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