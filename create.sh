#!/bin/sh
./ssc-admin.py tenants create rpa
./ssc-admin.py namespaces create rpa/manifesto
./ssc-admin.py topics create rpa/manifesto/q00DecodePDF
./ssc-admin.py topics create rpa/manifesto/q01DecodeTxt
./ssc-admin.py topics create rpa/manifesto/q02InjectMongo
./ssc-admin.py topics create rpa/manifesto/q99Erro

./ssc-admin.py functions \
                create \
                --name ConvertePDF2TXT \
                --tenant rpa \
                --namespace manifesto \
                --py ./src/ConvertePDF2TXT.py \
                --classname ConvertePDF2TXT.ConvertePDF2TXT \
                --inputs rpa/manifesto/q00DecodePDF \
                --output rpa/manifesto/q01DecodeTxt

./ssc-admin.py functions \
                create \
                --name ConvertTxt2Dic \
                --tenant rpa \
                --namespace manifesto \
                --py ./src/ConvertTxt2Dic.py \
                --classname ConvertTxt2Dic.ConvertTxt2Dic \
                --inputs rpa/manifesto/q01DecodeTxt \
                --output rpa/manifesto/q02InjectMongo

./ssc-admin.py functions \
                create \
                --name InjectMongoData \
                --tenant rpa \
                --namespace manifesto \
                --py ./src/InjectMongoData.py \
                --classname InjectMongoData.InjectMongoData \
                --inputs rpa/manifesto/q02InjectMongo 


# ./ssc-admin.py functions \
#                 delete \
#                 --name InjectMongoData \
#                 --tenant rpa \
#                 --namespace manifesto

# ./ssc-admin.py functions list --tenant rpa --namespace manifesto

# ./ssc-admin.py topics delete rpa/manifesto/q00DecodePDF
# ./ssc-admin.py topics delete rpa/manifesto/q01DecodeTxt
# ./ssc-admin.py topics delete rpa/manifesto/q02InjectMongo
# ./ssc-admin.py topics delete rpa/manifesto/q99Erro
# ./ssc-admin.py namespaces delete rpa/manifesto
# ./ssc-admin.py tenants delete rpa
