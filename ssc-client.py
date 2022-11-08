#!/usr/bin/env python3
'''
Created on 20220917
Update on 20221108
@author: Eduardo Pagotto
'''

import argparse
import logging
import time

from SSC.client.ClientQueue import ClientQueue
from SSC.subsys.GracefulKiller import GracefulKiller

def main():

    killer = GracefulKiller()

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s'
    )

    logging.getLogger('werkzeug').setLevel(logging.CRITICAL) 
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    log = logging.getLogger('ssc-client')

    client = ClientQueue('http://127.0.0.1:5152')

    try:
        parser = argparse.ArgumentParser(description='client commands')
        subparser = parser.add_subparsers(dest='command')

        produce = subparser.add_parser('produce')
        produce.add_argument('queue_name', type=str, help='Queue to process')
        produce.add_argument('--message', '-m', type=str, help='message a enviar', required=True)
        produce.add_argument('--number', '-n', type=int, help='numero repeticoes', required=False, default=1)
        
        consume = subparser.add_parser('consume')
        consume.add_argument('--name_app', '-s',type=str, help='message a enviar', required=True)
        consume.add_argument('--number', '-n', type=int, help='num receber', required=False, default=0)
        consume.add_argument('queue_name', type=str, help='Queue to process')
        args = parser.parse_args()

        if args.command == "produce":
            producer = client.create_producer(args.queue_name, 'ssc-client')
            for i in range(0, args.number):
                producer.send(args.message)

            #producer.close()

        elif args.command == "consume":

            consume = client.subscribe(args.queue_name)
            c = 0
            while killer.kill_now is False:
                val = consume.receive()
                if val != None:
                    log.info(str(val))
                    c += 1
                    if c > args.number:
                        break

                    continue
                else:
                    time.sleep(5)
            
            #consume.close()            

        else:
            log.error('Comando invalido')

    except Exception as exp:
        log.error(str(exp))
        exit(-1)
        
if __name__ == '__main__':
    main()