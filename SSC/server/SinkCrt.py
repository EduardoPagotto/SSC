'''
Created on 20221114
Update on 20221114
@author: Eduardo Pagotto
'''

from tinydb import TinyDB

from SSC.server.EnttCrt import EnttCrt
from SSC.server.SinkCocoon import SinkCocoon
from SSC.subsys.LockDB import LockDB

class SinkCrt(EnttCrt):

    def __init__(self, database : TinyDB, path_storage : str) -> None:
        super().__init__('sinks', database, path_storage)
        self.load_funcs_db()

    def create(self, params : dict) -> str:

        self.log.debug(f"create {params['name']}")

        params['storage'] = str(self.storage.resolve())
        with self.lock_func:
            for func in self.list_entt:
                if (params['tenant'] == func.document['tenant']) and (params['namespace'] == func.document['namespace']) and (params['name'] == func.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

            cocoon : SinkCocoon = SinkCocoon(self.colection_name, params, self.database)
            cocoon.start()
            self.list_entt.append(cocoon)

        return f"success create {params['name']}"

    def load_funcs_db(self):

        with LockDB(self.database, self.colection_name, False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                with self.lock_func:
                    self.log.debug(f'load from db: {params["name"]}')
                    cocoon : SinkCocoon = SinkCocoon(self.colection_name, params, self.database)
                    cocoon.start()
                    self.list_entt.append(cocoon)
            except:
                pass

