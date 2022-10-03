#!/bin/bash
		                    
./ssc-admin.py functions \
                create \
                func01 \
                ./funcs/FuncAdd.py \
                externo.FuncAdd.FuncAdd \
                queueInput \
                queueOutput 
