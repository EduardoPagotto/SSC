'''
Created on 20230308
Update on 20230313
@author: Eduardo Pagotto
'''

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from queue import Queue, Empty
from threading import Lock

from tinydb import TinyDB, Query
from tinydb.table import Document
from SSC.Message import Message

from SSC.subsys.LockDB import LockDB

def splitQueue(q_fulll_name : str) -> Tuple[str, str]: #TODO: implementar melhor
    lista = q_fulll_name.split('/')
    if len(lista) == 3:
        return lista[0] + '/' + lista[1], lista[2] 
    
    raise Exception(f'queue name {q_fulll_name} is invalid')

class Namespace(object):
    def __init__(self,  database : TinyDB, path_storage : str) -> None:
        self.database = database
        self.path_storage = path_storage
        self.table_name = 'namespaces'
        self.lock = Lock()
        self.map_queues : Dict[str, Queue] = {}

        self.log = logging.getLogger('SSC.Namespace')

        with LockDB(self.database, self.table_name, False) as table:
            itens = table.all()

        with self.lock: 
            for params in itens:
                ns = params['ns']
                self.log.debug(f'load from db {ns}: {params["queues"]}')
                for q in params['queues']:
                    self.map_queues[ns + '/' + q] = Queue()

    def summario(self) -> dict:
        res = {}
        for k, v in self.map_queues.items():
            res[k] = v.qsize()

        return res

    def load(self, ns : str) -> Document:

        self.log.info(f'namespace {ns} load')

        with LockDB(self.database, self.table_name) as table:
            q = Query()
            itens = table.search(q.ns == ns)
            if len(itens) > 0:
                return itens[0]

        raise Exception(f'namespace {ns} does not exist')    

    def create(self, ns : str) -> None:

        self.log.info(f'namespace {ns} create')
        try:
            self.load(ns)
        except:
            with LockDB(self.database, self.table_name, True) as table:
                id = table.insert({'ns': ns, 'queues':[]})

            self.log.info(f'namespace {ns} created id: {id}')
            return
        
        raise Exception(f'namespace {ns} already exists')
    

    def delete(self, namespace : str) -> None:

        self.log.debug(f'namespace {namespace} delete')

        doc = self.load(namespace)
        if len(doc['queues']) > 0:
            raise Exception(f'namespace {namespace} has queues: {str(doc["queues"])}')

        with LockDB(self.database, self.table_name, True) as table:
            table.remove(doc_ids=[doc.doc_id])
            self.log.info(f'namespace {namespace} deleted')   


    def list_all(self) -> List[str]:
        lista = []

        ns_list = []
        with LockDB(self.database, self.table_name) as table:
            ns_list = table.all()

        for val in ns_list:
            lista.append(val['ns'])

        return lista
    
    #-------

    def queue_list(self, namespace) -> List[str]:
        doc = self.load(namespace)
        return doc['queues']

    def queue_summario(self) -> List[dict]:

        lista : List[dict] = []
        with self.lock:
            for queue_name, queue in self.map_queues.items():
                lista.append({'queue': queue_name, 'size':queue.qsize()})

        return lista  


    def queue_create(self, queue_name_full):

        self.log.info(f'queue {queue_name_full} create')

        if queue_name_full in self.map_queues:
            raise Exception(f'queue {queue_name_full} already exists')
           
        ns, queue_name = splitQueue(queue_name_full)
        doc = self.load(ns)
        if queue_name in doc['queues']:
            raise Exception(f'queue {queue_name_full} already exists')

        doc['queues'].append(queue_name)
        with LockDB(self.database, self.table_name, True) as table:
            table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id])

        with self.lock:
            self.map_queues[queue_name_full] = Queue()

        self.log.info(f'queue create {queue_name_full} success')

    def queue_delete(self, queue_name_full) -> None:

        self.log.info(f'queue delete {queue_name_full}')

        for k, v in self.map_queues.items():
            if k == queue_name_full:
                with self.lock:
                    del self.map_queues[queue_name_full]

                break  

        namespace, queue = splitQueue(queue_name_full)
        doc = self.load(namespace)

        if queue in doc['queues']:
            doc['queues'].remove(queue)

            with LockDB(self.database, self.table_name, True) as table:
                table.update({'queues' : doc['queues']}, doc_ids=[doc.doc_id]) # TODO verificar se nao há functions anexado ao queue
                                
            self.log.info(f'queue delete {queue_name_full} success')    
            return

        raise Exception(f'queue delete {queue_name_full} does not exist')  


    def queues_get(self, field : str, params : Document) -> Dict[str, Queue]:

        novo : Dict[str, Queue] = {}
        if type(params[field]) == list: 
            for item in params[field]:
                novo[item] = self.queue_get(item)

        elif type(params[field]) == str:
           novo[params[field]] = self.queue_get(params[field])
           
        else:
            raise Exception('queue name invalid ' + str(params[field]))
        
        return novo

    def queue_get(self, queue_name_full) -> Queue:

        try:
            with self.lock:
                return self.map_queues[queue_name_full]
        except:

            self.log.info(f'load queue from DB {queue_name_full}')
            ns, queue_find = splitQueue(queue_name_full)
            doc = self.load(ns)

            if queue_find in doc['queues']:

                q : Queue = Queue()

                with self.lock:
                    self.map_queues[queue_name_full] = q
               
                return q

        raise Exception(f'queue {queue_name_full} not exist in DB')

        
    def push(self, key : str, prop: dict, queue_name : str, msg_str : str): # FIXME: criar message antes como em QueueProdCons!!!!!

        msg = Message.create(seq_id = 0,
                                payload = msg_str,
                                queue = queue_name,
                                properties = prop,
                                producer = "client",
                                key = key)

        self.queue_get(queue_name).put(json.dumps(msg.to_dict()))

    def pop(self, queue_name : str, timeOut: int) -> Optional[Any]:  # FIXME: criar message antes como em QueueProdCons!!!!!
        return self.queue_get(queue_name).get(block=True, timeout=timeOut)
