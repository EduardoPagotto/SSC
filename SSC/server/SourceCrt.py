'''
Created on 20221108
Update on 20221110
@author: Eduardo Pagotto
'''

from tinydb import TinyDB

from SSC.server.EnttCrt import EnttCrt
from SSC.server.SourceCocoon import SourceCocoon
from SSC.subsys.LockDB import LockDB

class SourceCrt(EnttCrt):

    def __init__(self, database : TinyDB, path_storage : str) -> None:
        super().__init__('sources', database, path_storage)
        self.load_connectors_db()

    def create(self, params : dict) -> str:

        self.log.debug(f"create {params['name']}")
        with self.lock_func:
            for source in self.list_entt:
                if (params['tenant'] == source.document['tenant']) and (params['namespace'] == source.document['namespace']) and (params['name'] == source.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

            cocoon : SourceCocoon = SourceCocoon(self.colection_name, params, self.database)
            cocoon.start()
            self.list_entt.append(cocoon)

        return f"success create {params['name']}"

    def load_connectors_db(self):

        with LockDB(self.database, self.colection_name, False) as table:
            itens = table.all()
            
        for params in itens:
            try:
                with self.lock_func:
                    self.log.debug(f'load from db: {params["name"]}')
                    cocoon : SourceCocoon = SourceCocoon(self.colection_name, params, self.database)
                    cocoon.start()
                    self.list_entt.append(cocoon)
            except:
                pass
