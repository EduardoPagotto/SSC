#!/bin/bash
		                    
./ssc-admin.py functions \
                create \
                --name name01 \
                --tenant rpa \
                --namespace manifesto \
                --py ./funcs/FuncAdd.py \
                --classname FuncAdd.FuncAdd \
                --inputs rpa/manifesto/queue01 \
                --output rpa/manifesto/queue02 



