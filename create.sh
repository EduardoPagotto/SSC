#!/bin/bash
		                    
# ./ssc-admin.py functions \
#                 create \
#                 --name name01 \
#                 --tenant rpa \
#                 --namespace manifesto \
#                 --py ./funcs/FuncAdd.py \
#                 --classname FuncAdd.FuncAdd \
#                 --inputs rpa/manifesto/queue01 \
#                 --output rpa/manifesto/queue02 

./ssc-admin.py functions \
                create \
                --name ConvertePDF2TXT \
                --tenant rpa \
                --namespace manifesto \
                --py ./funcs/ConvertePDF2TXT.py \
                --classname ConvertePDF2TXT.ConvertePDF2TXT \
                --inputs rpa/manifest/q00DecodePDF \
                --output rpa/manifest/q01DecodeTxt

./ssc-admin.py functions \
                create \
                --name ConvertTxt2Dic \
                --tenant rpa \
                --namespace manifesto \
                --py ./funcs/ConvertTxt2Dic.py \
                --classname ConvertTxt2Dic.ConvertTxt2Dic \
                --inputs rpa/manifest/q01DecodeTxt \
                --output rpa/manifest/q02InjectMongo

./ssc-admin.py functions \
                create \
                --name InjectMongoData \
                --tenant rpa \
                --namespace manifesto \
                --py ./funcs/InjectMongoData.py \
                --classname InjectMongoData.InjectMongoData \
                --inputs rpa/manifest/q02InjectMongo 


