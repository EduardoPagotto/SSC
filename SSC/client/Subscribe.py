

import json
import pathlib
import requests

from sJsonRpc.ProxyObject import ProxyObject
from sJsonRpc.ConnectionControl import ConnectionControl


class ConnectionRestApiSubscribe(ConnectionControl):
    def __init__(self, addr : str):
        super().__init__(addr)

    def exec(self, input_rpc : dict, *args, **kargs) -> dict:
        url : str
        headers : dict= {'rpc-Json': json.dumps(input_rpc)}
        payload : dict ={}

        # comandos rpc's
        url = self.getUrl() + "/client-subscribe"
        files = None
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        if response.status_code != 201:
            raise Exception(response.text)

        return json.loads(response.text) # dict do rpcjson


class Subscribe(object):
    def __init__(self, id : int, s_address : str, topic : str) -> None:
        self.restAPI = ConnectionRestApiSubscribe(s_address)
        self.topic = topic
        self.id = id

    def __rpc(self):
        return ProxyObject(self.restAPI)

    def close(self):
        self.__rpc().close()

    def receive(self, timeOut : int): # se 0 bloqueia, maior raise quando estoura 
        return self.__rpc().subscribe_receive(self.id, timeOut)


