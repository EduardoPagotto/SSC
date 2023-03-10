'''
Created on 20221108
Update on 20221114
@author: Eduardo Pagotto
'''

from SSC.server.Namespace import Namespace
from SSC.server.EnttCrt import EnttCrt
from SSC.server.SourceCocoon import SourceCocoon
from SSC.subsys.LockDB import LockDB

class SourceCrt(EnttCrt):

    def __init__(self, namespace : Namespace) -> None:
        super().__init__('sources', namespace)
        self.load_connectors_db()

    def create(self, params : dict) -> str:

        self.log.debug(f"create {params['name']}")

        params['storage'] = str(self.ns.path_storage)
        with self.lock_func:
            for source in self.list_entt:
                if (params['namespace'] == source.document['namespace']) and (params['name'] == source.document['name']):
                    raise Exception(f'topic {params["name"]} already exists')

            cocoon : SourceCocoon = SourceCocoon(self.colection_name, params, self.ns)
            cocoon.start()
            self.list_entt.append(cocoon)

        return f"success create {params['name']}"

    def load_connectors_db(self):

        with LockDB(self.ns.database, self.colection_name, False) as table:
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
