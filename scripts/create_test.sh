#!/bin/sh--namespace ns01

# --- Cria tenant/namespace/topic 
./ssc-admin.py namespaces create test/ns01
./ssc-admin.py queues create test/ns01/queue01
./ssc-admin.py queues create test/ns01/queue02
./ssc-admin.py queues create test/ns01/queue03

# -- Sources --

# gera mensagens sequenciais para debug na queue test/ns01/queue01
./ssc-admin.py sources create \
                --name dummy-teste \
                --destinationtopicname test/ns01/queue01 \
                --archive ./builtin/sources/Dummy/Dummy.py \
                --classname Dummy.Dummy \
                --namespace test/ns01 \
                --sourceconfigfile ./builtin/etc/source_dummy.yaml

# watch dir pega arquivos estruturados em diretorios enviando para queue test/ns01/queue01
./ssc-admin.py sources create \
                --name watch1 \
                --destinationtopicname test/ns01/queue01 \
                --archive ./builtin/sources/Watchdogdir/Watchdogdir.py \
                --classname Watchdogdir.Watchdogdir \
                --namespace test/ns01 \
                --sourceconfigfile ./builtin/etc/watchdogdir_cfg.yaml

# list 
./ssc-admin.py sources list --namespace test/ns01

# -- Sinks --

# pega os dados da test/ns01/queue02 e os envia para um json em arquivo pelo TinyDB
./ssc-admin.py sinks create \
                --name tiny-teste \
                --namespace test/ns01 \
                --archive ./builtin/sinks/SinkTinydb/SinkTinydb.py \
                --classname SinkTinydb.SinkTinydb \
                --inputs test/ns01/queue02 \
                --sinkconfigfile ./builtin/etc/sink_tinydb.yaml 

# pega os dados da test/ns01/queue02 e os envia para um csv em arquivo
./ssc-admin.py sinks create \
                --name csv-teste \
                --namespace test/ns01 \
                --archive ./builtin/sinks/SinkCSV/SinkCSV.py \
                --classname SinkCSV.SinkCSV \
                --inputs test/ns01/queue03 \
                --sinkconfigfile ./builtin/etc/sink_csv.yaml 

# sink de gravacao de arquivos em diretorio
./ssc-admin.py sinks create \
                --name writer-test \
                --namespace test/ns01 \
                --archive ./builtin/sinks/SinkWriterFiles/SinkWriterFiles.py \
                --classname SinkWriterFiles.SinkWriterFiles \
                --inputs test/ns01/queue04 \
                --sinkconfigfile ./builtin/etc/sink_writerfiles.yaml 


# -- Functions --

# cria function para Relay da fila inputs test/ns01/queue01 para test/ns01/queue02
./ssc-admin.py functions create \
                --name relay01 \
                --namespace test/ns01 \
                --py ./builtin/functions/Relay.py \
                --classname Relay.Relay \
                --inputs test/ns01/queue01 \
                --output test/ns01/queue02


# Teste de client/producer
#./ssc-client.py produce test/ns01/queue01 -m "teste 123..." -n 2
#./ssc-client.py consume -s appteste test/ns01/queue01 -n 1