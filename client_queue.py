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

        client = ClientQueue('http://127.0.0.1:5151')
        producer = client.create_producer('my-topic')
        consumer = client.subscribe('my-topic')

        msg_1 = json.dumps({'id': 0,
                            'create': datetime.now(tz=timezone.utc).timestamp(),
                            'codeId': 10,
                            'erros':[]})

        producer.send(msg_1)

        msg = consumer.receive(100)

        log.debug(msg)
        # producer.send(str.encode(json.dumps({'id': id, 
        #                                     'create': datetime.now(tz=timezone.utc).timestamp(),
        #                                     'codeId': 10,
        #                                     'erros':[]})))



        # import pulsar

        # client = pulsar.Client('pulsar://localhost:6650')

        # producer = client.create_producer('my-topic')

        # for i in range(10):
        #     producer.send(('Hello-%d' % i).encode('utf-8'))

        # client.close()


        # import pulsar

        # client = pulsar.Client('pulsar://localhost:6650')

        # consumer = client.subscribe('my-topic', 'my-subscription')

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




        #     client.cleanAt(0, 0, 5)

        #     id = 1000
        #     ggg = client.info(id)
        #     ttt = client.remove(id)
        #     kkk = client.keep(id)
        #     valid, msg_erro = client.download(id, './testez1.jpg')
        #     log.debug(f'Id:{id}: msg:{msg_erro} info:{str(client.info(id))}')

        #     count=0
        #     while (count < 500):

        #         id, msg = client.upload('./data/disco1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg} info:{str(client.info(id))}')

        #         id, msg = client.upload('./data/disco1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg} info:{str(client.info(id))}')

        #         zzz = client.remove(id)

        #         valid, msg_erro = client.download(id, './testez1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg_erro} info:{str(client.info(id))}')

        #         id, msg = client.upload('./data/disco1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg} info:{str(client.info(id))}')

        #         # valid, msg_erro = client.download(id, '.')
        #         # log.debug(f'Id:{id}: msg:{msg_erro} info:{str(client.info(id))}')

        #         valid, msg_erro = client.download(id, './testez1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg_erro} info:{str(client.info(id))}')

        #         id, msg = client.upload('./data/disco1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg} info:{str(client.info(id))}')

        #         id, msg = client.upload('./data/disco1.jpg')
        #         log.debug(f'Id:{id}: msg:{msg} info:{str(client.info(id))}')

        #         client.keep(id)
        #         log.debug(f'Id {id}: {str(client.info(id))}')

        #         #time.sleep(10)

        #         count += 1
        


        # manager = NamedQueue()

        # manager.create('q1')
        # manager.create('q1')
        # manager.create('q2')
        # manager.create('q3')

        # log.debug(str(list(manager.get_list()))) 

        # q1 = manager.get('q1')
        # q2 = manager.get('q2')
        # q3 = manager.get('ZZZ')


        # imp = DRegistry()
        # imp.enable_plugin('funcoes.externo.FuncAdd')

        # val = imp.registered_plugin.metodo1('Locutus vive!!!!')
        # val2 = val

        # imp.log.debug(val)
        # imp.log.debug(val2)


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