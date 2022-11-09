'''
Created on 20221108
Update on 20221109
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
        self.list_connector : List[ConnectorCocoon] = []

        self.log = logging.getLogger('SSC.ConnectorCrt')

        self.load_connectors_db()

    def summario(self) ->dict:
        
        result = []
        tot = 0
        with self.lock_func:
            tot = len(self.list_connector)  
            for i in self.list_connector:
                result.append(i.sumary())  

        return {'online': tot, 'tasks': result}

    def create(self, params : dict) -> str:

        self.log.debug(f"function create {params['name']}")
        with self.lock_func:
            for conn in self.list_connector:
                if (params['tenant'] == conn.document['tenant']) and (params['namespace'] == conn.document['namespace']) and (params['name'] == conn.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

            cocoon : ConnectorCocoon = ConnectorCocoon(params, self.database)
            cocoon.start()
            self.list_connector.append(cocoon)

        return f"success create {params['name']}"

    def stop_func_all(self):

        with self.lock_func:
            for conn in self.list_connector:
                conn.stop()

            for conn in self.list_connector:
                conn.join()
                self.list_connector.remove(conn)

    def pause_resume(self, func_name : str, is_pause : bool):
        tenant, namespace, name = splitTopic(func_name)   
        with self.lock_func:
            for fun in self.list_connector:
                if ((fun.document['tenant'] == tenant) and (fun.document['namespace'] == namespace) and (fun.document['name'] == name)):
                    msg : str = ''
                    if is_pause:
                        msg = f'conn {name} paused'
                        fun.pause()
                    else:
                        msg = f'conn {name} resumed'
                        fun.resume()

                    self.log.info(msg)
                    return msg

        raise Exception(f'function {func_name} does not exist')

    def delete(self, func_name : str):

        self.log.debug(f'function delete {func_name}')

        tenant, namespace, name = splitTopic(func_name)
        funcValid = None

        with self.lock_func:
            for conn in self.list_connector:
                if (tenant == conn.document['tenant']) and (namespace == conn.document['namespace']) and (name == conn.document['name']):
                    self.list_connector.remove(conn)
                    funcValid = conn

        if funcValid:            
            funcValid.stop()
            funcValid.join()
            with LockDB(self.database, 'connectors', True) as table:
                table.remove(doc_ids=[funcValid.document.doc_id])

            return f'success delete {func_name}'

        raise Exception(f'function {func_name} does not exist')

    def list_all(self, tenant_ns : str) -> List[str]:
        with LockDB(self.database, 'connectors', False) as table:
            itens = table.all()

        lista : List[str] = []
        for item in itens:
            tenant, namespace = splitNamespace(tenant_ns)
            if (tenant == item['tenant']) and (namespace == item['namespace']): 
                lista.append(item['name'])

        return lista

    def load_connectors_db(self):

        with LockDB(self.database, 'connectors', False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                with self.lock_func:
                    self.log.debug(f'function load from db: {params["name"]}')
                    cocoon : ConnectorCocoon = ConnectorCocoon(params, self.database)
                    cocoon.start()
                    self.list_connector.append(cocoon)
            except:
                pass


    def execute(self) -> Tuple[int, int]:

        tot_proc = 0
        tot_erro = 0
        with self.lock_func:
            for conn in self.list_connector:
                ok, erro = conn.count_tot()
                tot_proc += ok
                tot_erro += erro

        return tot_proc, tot_erro
