'''
Created on 20221006
Update on 20230313
@author: Eduardo Pagotto
'''

from SSC.server.Namespace import Namespace
from SSC.server.EnttCrt import EnttCrt
from SSC.server.FuncCocoon import FuncCocoon
from SSC.subsys.LockDB import LockDB

class FunctionCrt(EnttCrt):

    def __init__(self, sufix : str, namespace : Namespace) -> None:
        super().__init__(sufix, namespace)
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

