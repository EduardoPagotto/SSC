#!/bin/sh--namespace ns01

# --- Cria namespace e queues
./ssc-admin.py namespaces create test/ns01
./ssc-admin.py queues create test/ns01/queue01
./ssc-admin.py queues create test/ns01/queue02
./ssc-admin.py queues create test/ns01/queue03
./ssc-admin.py queues create test/ns01/queue04
./ssc-admin.py queues create test/ns01/queue05

# -- Sources --
# gera mensagens sequenciais para debug na queue test/ns01/queue01
./ssc-admin.py functions create \
                --name SrcDummy \
                --namespace test/ns01 \
                --classname SrcDummy.SrcDummy \
                --py ./builtin/SrcDummy.py \
                --timeout 5.0 \
                --output test/ns01/queue01 

#./ssc-admin.py functions delete --name SrcDummy --namespace test/ns01

# watch dir pega arquivos estruturados em diretorios enviando para queue test/ns01/queue01
./ssc-admin.py functions create \
                --name watch1 \
                --namespace test/ns01 \
                --classname SrcWatchdogdir.SrcWatchdogdir \
                --py ./builtin/SrcWatchdogdir.py \
                --configfile ./builtin/etc/watchdogdir_cfg.yaml \
                --timeout 5.0 \
                --output test/ns01/queue01 

# Relay da queue test/ns01/queue03 para o redis
./ssc-admin.py functions create \
                --name source_relay_redis01 \
                --namespace test/ns01 \
                --classname SrcRedisQueue.SrcRedisQueue \
                --py ./builtin/SrcRedisQueue.py \
                --configfile ./builtin/etc/source_redis_queue.yaml \
                --timeout 5.0 \
                --output test/ns01/queue05

./ssc-admin.py functions delete --name source_relay_redis01 --namespace test/ns01

# list 
./ssc-admin.py functions list --namespace test/ns01

# -- Sinks --
# pega os dados da test/ns01/queue02 e os envia para um json em arquivo pelo TinyDB
./ssc-admin.py functions create \
                --name tiny-teste \
                --namespace test/ns01 \
                --classname SinkTinydb.SinkTinydb \
                --py ./builtin/SinkTinydb.py \
                --configfile ./builtin/etc/sink_tinydb.yaml \
                --timeout 5.0 \
                --inputs test/ns01/queue02 

./ssc-admin.py functions delete --name tiny-teste --namespace test/ns01

# pega os dados da test/ns01/queue02 e os envia para um csv em arquivo
./ssc-admin.py functions create \
                --name csv-teste \
                --namespace test/ns01 \
                --classname SinkCSV.SinkCSV \
                --py ./builtin//SinkCSV.py \
                --configfile ./builtin/etc/sink_csv.yaml \
                --timeout 5.0 \
                --inputs test/ns01/queue03 

# sink de gravacao de arquivos em diretorio
./ssc-admin.py functions create \
                --name writer-test \
                --namespace test/ns01 \
                --classname SinkWriterFiles.SinkWriterFiles \
                --py ./builtin/SinkWriterFiles.py \
                --configfile ./builtin/etc/sink_writerfiles.yaml \
                --inputs test/ns01/queue04

# sink para redis queue
./ssc-admin.py functions create \
                --name sink_redis_queue01 \
                --namespace test/ns01 \
                --classname SinkRedisQueue.SinkRedisQueue \
                --py ./builtin/SinkRedisQueue.py \
                --configfile ./builtin/etc/sink_redis_queue.yaml \
                --inputs test/ns01/queue02

/ssc-admin.py functions delete --name writer-test --namespace test/ns01


# -- Functions --
# cria function para Relay da fila inputs test/ns01/queue01 para test/ns01/queue02
./ssc-admin.py functions create \
                --name FuncRelay01 \
                --namespace test/ns01 \
                --classname FuncRelay.FuncRelay \
                --py ./builtin/FuncRelay.py \
                --timeout 1.0 \
                --inputs test/ns01/queue01 \
                --output test/ns01/queue02

./ssc-admin.py functions delete --name FuncRelay01 --namespace test/ns01

# Teste de client/producer
./ssc-client.py produce test/ns01/queue01 -m "teste 123..." -n 2 --key "0010201010" --properties "{\"val1\":\"aaa\"}"
./ssc-client.py consume -s appteste test/ns01/queue01 -n 1