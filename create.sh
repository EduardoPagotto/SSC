#!/bin/bash
		                    
./ssc-admin.py functions \
                create \
                --name name01 \
                --py ./funcs/FuncAdd.py \
                --classname externo.FuncAdd.FuncAdd \
                --inputs queue01 \
                --output queue02 



