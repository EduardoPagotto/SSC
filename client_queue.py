#!/usr/bin/env python3
'''
Created on 20220917
Update on 20220927
@author: Eduardo Pagotto
'''

from datetime import datetime, timezone
import json
import logging
from SSC.client.ClientQueue import ClientQueue

def main():

    try:
        log = logging.getLogger('Client')

        client = ClientQueue('http://127.0.0.1:5152')
        producer = client.create_producer('test/ns01/queue01')
        consumer = client.subscribe('test/ns01/queue01')

        msg_1 = json.dumps({'id': 0,
                            'create': datetime.now(tz=timezone.utc).timestamp(),
                            'codeId': 10,
                            'erros':[]})

        producer.send(msg_1)

        msg = consumer.receive(5)

        msg2 = consumer.receive()

        log.debug(msg)
        # producer.send(str.encode(json.dumps({'id': id, 
        #                                     'create': datetime.now(tz=timezone.utc).timestamp(),
        #                                     'codeId': 10,
        #                                     'erros':[]})))



        # import pulsar

        # client = pulsar.Client('pulsar://localhost:6650')

        # producer = client.create_producer('queue01')

        # for i in range(10):
        #     producer.send(('Hello-%d' % i).encode('utf-8'))

        # client.close()


        # import pulsar

        # client = pulsar.Client('pulsar://localhost:6650')

        # consumer = client.subscribe('queue01', 'my-subscription')

        # while True:
        #     msg = consumer.receive()
        #     try:
        #         print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))
        #         # Acknowledge succeSSCul processing of the message
        #         consumer.acknowledge(msg)
        #     except Exception:
        #         # Message failed to be processed
        #         consumer.negative_acknowledge(msg)

        # client.close()





        # classname: funcoes.externo.Relay
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
        #   --inputs "persistent://rpa/manifesto/q01DecodeTxt"  \
        #   --output "persistent://rpa/manifesto/q99Erro" \
        #   --parallelism 1 


    except Exception as exp:
        log.error('{0}'.format(str(exp)))

    log.info('App desconectado')

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(threadName)-16s %(funcName)-20s %(message)s',
        datefmt='%H:%M:%S',
    )

    logging.getLogger('werkzeug').setLevel(logging.CRITICAL) 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    main()