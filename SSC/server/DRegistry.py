
import importlib
import logging
import pathlib
import time

from threading import Lock, Thread
from typing import Any

from tinydb import TinyDB, Query

from  sJsonRpc.RPC_Responser import RPC_Responser

from SSC.server.NamedQueue import NamedQueue
from .__init__ import __version__ as VERSION
from .__init__ import __date_deploy__ as DEPLOY

class DRegistry(RPC_Responser):
    def __init__(self, path_db : str, path_storage : str) -> None:
        super().__init__(self)

        self.lock_db = Lock()

        path1 = pathlib.Path(path_db)
        path1.mkdir(parents=True, exist_ok=True)
        self.db = TinyDB(str(path1) + '/master.json')

        self.storage = pathlib.Path(path_storage)
        self.storage.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(threadName)-16s %(funcName)-20s %(message)s',
            datefmt='%H:%M:%S',
        )

        self.done : bool = False
        self.ticktack = 0

        logging.getLogger('werkzeug').setLevel(logging.CRITICAL)

        self.log = logging.getLogger('SSC')
        self.log.info(f'>>>>>> SSC v-{VERSION} ({DEPLOY}), DB: {str(path1)} Storage: {str(self.storage)}')

        self.registered_plugin = None

        self.named_queues = NamedQueue()

        with self.lock_db:
            table = self.db.table('topics')
            lista = table.all()
            for item in lista:
                self.named_queues.create(str(item.doc_id))         

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()



    def cleanner(self) ->None:
        """[Garbage collector of files]
        """

        time.sleep(10)
        self.log.info('thread cleanner_files start')
        while self.done is False:

            # if (self.ticktack % 12) == 0:

            #     now = datetime.now(tz=timezone.utc)
            #     limit = (now - self.delta).timestamp()

            #     with self.lock_db:
            #         q = Query()
            #         itens = self.db.search(q.last < limit)

            #     ll = []
            #     for val in itens:
            #         ll.append(val.doc_id)
            #         file = pathlib.Path(val['internal'])
            #         file.unlink(missing_ok=True)

            #         self.log.debug(f"Remove Id:{val.doc_id}, {val['internal']}")
            #         self.tot_out += 1

            #     if len(ll) > 0:
            #         with self.lock_db:
            #             self.db.remove(doc_ids=ll)

            self.log.debug(f'Tick-Tack... ')

            self.ticktack += 1
            time.sleep(5)

        self.log.info('thread cleanner_files stop')


    def enable_plugin(self, plugin : str):
            """Enable a ingester plugin for use parsing design documents.

            :params plugin: - A string naming a class object denoting the ingester plugin to be enabled
            """

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)
                self.registered_plugin = klass(1)

            except Exception as ex:
                self.log.error(
                    "Could not enable plugin %s - %s" % (plugin, str(ex)))

            if self.registered_plugin is None:
                self.log.error("Could not enable at least one plugin")
                raise Exception("Could not enable at least one plugin") 


    def create_producer(self, topic):

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)
            if len(itens) == 0:
                raise Exception('Topico nao existe: ' + topic)
                #return table.insert({'topic': topic, 'name_app':'', 'user_config':''})

            name = str(itens[0].doc_id)
            if self.named_queues.exist(name) is False: 
                self.named_queues.create(name)

            return itens[0].doc_id
                                               

    def subscribe(self, topic):

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)
            if len(itens) == 0:
                raise Exception('Topico nao existe: ' + topic)
                #return table.insert({'topic': topic, 'name_app':'', 'user_config':''})

            name = str(itens[0].doc_id)
            if self.named_queues.exist(name) is False: 
                self.named_queues.create(name)

            return itens[0].doc_id

    def send_producer(self, id : int, msg : str):
        self.named_queues.push(str(id), msg)

    def subscribe_receive(self, id: int, timeOut: int) -> Any:
        return self.named_queues.pop(str(id))


        # classname: funcoes.externo.FuncAdd
        # input: qname
        # output: qname
        # --py /var/app/src/ConvertTxt2Dic.py ??? copiar ???
        #--user-config '{"FileCfg":"aaaaa"}'
        #--user-config-file "/pulsar/host/etc/func1.yaml"


        # /pulsar/bin/pulsar-admin functions create \
        #   --tenant rpa \
        #   --namespace manifest \
        #   --name ConvertTxt2Dic \
        #   --py /var/app/src/ConvertTxt2Dic.py \
        #   --classname ConvertTxt2Dic.ConvertTxt2Dic \
        #   --inputs "persistent://rpa/manifest/q01DecodeTxt"  \
        #   --output "persistent://rpa/manifest/q99Erro" \
        #   --parallelism 1 