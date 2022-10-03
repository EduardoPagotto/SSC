

import importlib
import logging
import pathlib
import shutil
import time

from threading import Lock, Thread
from typing import Any, Dict, List

from tinydb import TinyDB, Query

from  sJsonRpc.RPC_Responser import RPC_Responser

from SSC.Function import Function


from .__init__ import __version__ as VERSION
from .__init__ import __date_deploy__ as DEPLOY

from queue import Queue, Empty
from typing import Any, Optional

class Topic(object):
    def __init__(self, id : int, name: str) -> None:
        self.name = name
        self.id = id
        self.queue : Queue = Queue()

    def push(self, value : Any) -> None:
         self.queue.put(value)

    def pop(self, timeout: int) -> Optional[Any]:
        try:
            if timeout == 0:
                return self.queue.get_nowait()
            else:
                return self.queue.get(block=True, timeout=timeout)
        except Empty:
            pass

        return None

    def qsize(self) -> int:
        return self.queue.qsize()

    def empty(self) -> bool:
        return self.queue.empty()



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

        self.func_list : List[Function] = []
        self.map_topics : Dict[int, Topic] = {}

        self.t_cleanner : Thread = Thread(target=self.cleanner, name='cleanner_files')
        self.t_cleanner.start()

    def cleanner(self) ->None:
        """[Garbage collector of files]
        """

        time.sleep(10)
        self.log.info('thread cleanner_files start')
        while self.done is False:

            for obj in self.func_list:
                if obj.qIn > 0:
                    res = self.subscribe_receive(obj.qIn, 0)
                    if res != None:
                        ret = obj.process(res, {})
                        if (obj.qOut != -1) and (ret != None):
                            self.send_producer(obj.qOut, ret)

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


    def function_load(self, plugin : str) -> Any:
            """Enable a ingester plugin for use parsing design documents.

            :params plugin: - A string naming a class object denoting the ingester plugin to be enabled
            """
            klass = None

            if plugin is None or plugin == '':
                self.log.error("Cannot have an empty plugin string.")

            try:
                (module, x, classname) = plugin.rpartition('.')

                if module == '':
                    raise Exception()
                mod = importlib.import_module(module)
                klass = getattr(mod, classname)
                # self.func_list = klass()
                #self.nova_func = klass()
                #self.nova_func.

            except Exception as ex:
                self.log.error("Could not enable class %s - %s" % (plugin, str(ex)))
                raise ex

            if klass is None:
                self.log.error(f"Could not enable at least one class: {plugin}")
                raise Exception(f"Could not enable at least one class: {plugin}") 

            return klass()

    # ClientQueue
    def create_producer(self, topic : str) -> int:

        for k, v in self.map_topics.items():
            if v.name == topic:
                return v.id

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)

        if len(itens) == 1:
            id = itens[0].doc_id
            self.map_topics[id] = Topic(id, topic)
            return id

        raise Exception(f'topic {topic} does not exist')
                                               
    # ClientQueue
    def subscribe(self, topic):

        for k, v in self.map_topics.items():
            if v.name == topic:
                return v.id

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)

        if len(itens) == 1:
            id = itens[0].doc_id
            self.map_topics[id] = Topic(id, topic)
            return id

        raise Exception(f'topic {topic} does not exist')

    # Producer
    def send_producer(self, id : int, msg : str):
        self.map_topics[id].push(msg)

    # Subscribe
    def subscribe_receive(self, id: int, timeOut: int) -> Optional[Any]:
        return self.map_topics[id].pop(timeOut)

    # Admin
    def topics_create(self, topic : str) -> str:

        for k, v in self.map_topics.items():
            if v.name == topic:
                return f'topic {topic} already exists'

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)
            if len(itens) == 0:

                id = table.insert({'topic': topic, 'name_app':'', 'user_config':''})

                self.map_topics[id] = Topic(id, topic)
                return 'Sucess ' + topic

            return f'topic {topic} already exists'

    # Admin
    def topics_delete(self, topic : str, force : bool) -> str:

        for k, v in self.map_topics.items():
            if v.name == topic:
                if force is False:
                    return f'topic {topic} is in use'
                else:
                    del self.map_topics[k]
                    with self.lock_db:
                        table = self.db.table('topics')
                        table.remove(doc_ids=[k])

                    return f'topic {topic} deleted in use'

        with self.lock_db:
            table = self.db.table('topics')
            q = Query()
            itens = table.search(q.topic == topic)
            if len(itens) == 0:
                return f'topic {topic} does not exist'

            id = itens[0].doc_id
            table.remove(doc_ids=[id])
            return f'Topico {topic} removido'

    # Admin
    def function_create(self, params: dict):
        self.log.debug('Create ')

        idQueueIn : int = -1
        idQueueOut : int = -1
        
        try:
            if 'input' in params:
                idQueueIn = self.subscribe(params['input'])
                params['idQueueIn'] = idQueueIn

            if 'output' in params:
                idQueueOut = self.create_producer(params['output'])
                params['idQueueOut'] = idQueueOut
            

            # copicar pgm para area interna
            path_file_src = pathlib.Path(params['pgm'])

            names = params['class'].split('.')

            path_dest = pathlib.Path(str(self.storage) + '/' + names[0])

            path_dest.mkdir(parents=True, exist_ok=True)
            final = str(path_dest) + '/' + path_file_src.name

            params['final'] = final
            shutil.copy(str(path_file_src), final)


            base = str(self.storage).replace('/','.') + '.' + params['class']
            klass : Function = self.function_load(base)
            klass.name = params['name']
            klass.qIn = idQueueIn
            klass.qOut = idQueueOut
            klass.useConfig = {}

            with self.lock_db:
                table = self.db.table('funcs')
                table.insert(params)

            self.func_list.append(klass)

            return f"function {params['name']} created success"

        except Exception as exp:
            return str(exp.args[0])

        return 'Nao implementado'




        # classname: funcoes.externo.FuncAdd
        # input: qname
        # output: qname
        # --py /var/app/src/ConvertTxt2Dic.py ??? copiar ???
        #--user-config '{"FileCfg":"aaaaa"}'
        #--user-config-file "/pulsar/host/etc/func1.yaml"


        # /pulsar/bin/pulsar-admin functions create \
        #   --name ConvertTxt2Dic \
        #   --py /var/app/src/ConvertTxt2Dic.py \
        #   --classname ConvertTxt2Dic.ConvertTxt2Dic \
        #   --inputs "persistent://rpa/manifest/q01DecodeTxt"  \
        #   --output "persistent://rpa/manifest/q99Erro" \
        #   --parallelism 1 