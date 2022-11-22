#!/bin/sh

# --- Cria tenant/namespace/topic 
./ssc-admin.py tenants create test
./ssc-admin.py namespaces create test/ns01
./ssc-admin.py topics create test/ns01/queue01
./ssc-admin.py topics create test/ns01/queue02
./ssc-admin.py topics create test/ns01/queue03

# -- Sources --

# gera mensagens sequenciais para debug na queue test/ns01/queue01
./ssc-admin.py sources create \
                --name dummy-teste \
                --destinationtopicname test/ns01/queue01 \
                --archive ./plugins/sources/Dummy/Dummy.py \
                --classname Dummy.Dummy \
                --tenant test \
                --namespace ns01 \
                --sourceconfigfile ./plugins/etc/source_dummy.yaml

# watch dir pega arquivos estruturados em diretorios enviando para queue test/ns01/queue01
./ssc-admin.py sources create \
                --name watch1 \
                --destinationtopicname test/ns01/queue01 \
                --archive ./plugins/sources/Watchdogdir/Watchdogdir.py \
                --classname Watchdogdir.Watchdogdir \
                --tenant test \
                --namespace ns01 \
                --sourceconfigfile ./plugins/etc/watchdogdir_cfg.yaml


# -- Functions --

# cria function para relar da fila inputs test/ns01/queue01 para test/ns01/queue02
./ssc-admin.py functions create \
                --name name01 \
                --tenant test \
                --namespace ns01 \
                --py ./plugins/functions/Relay.py \
                --classname Relay.Relay \
                --inputs test/ns01/queue01 \
                --output test/ns01/queue02

# -- Sinks --

# pega os dados da test/ns01/queue02 e os envia para um json em arquivo pelo TinyDB
./ssc-admin.py sinks create \
                --name tiny-teste \
                --tenant test \
                --namespace ns01 \
                --archive ./plugins/sinks/SinkTinydb/SinkTinydb.py \
                --classname SinkTinydb.SinkTinydb \
                --inputs test/ns01/queue02 \
                --sinkconfigfile ./plugins/etc/sink_tinydb.yaml 

# pega os dados da test/ns01/queue02 e os envia para um csv em arquivo
./ssc-admin.py sinks create \
                --name csv-teste \
                --tenant test \
                --namespace ns01 \
                --archive ./plugins/sinks/SinkCSV/SinkCSV.py \
                --classname SinkCSV.SinkCSV \
                --inputs test/ns01/queue02 \
                --sinkconfigfile ./plugins/etc/sink_csv.yaml 

# sink de gravacao de arquivos em diretorio
./ssc-admin.py sinks create \
                --name writer-test \
                --tenant test \
                --namespace ns01 \
                --archive ./plugins/sinks/SinkWriterFiles/SinkWriterFiles.py \
                --classname SinkWriterFiles.SinkWriterFiles \
                --inputs test/ns01/queue03 \
                --sinkconfigfile ./plugins/etc/sink_writerfiles.yaml 
