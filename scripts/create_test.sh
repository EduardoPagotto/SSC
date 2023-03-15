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
./ssc-admin.py sources create \
                --name dummy-teste \
                --namespace test/ns01 \
                --classname Dummy.Dummy \
                --py ./builtin/sources/Dummy/Dummy.py \
                --timeout 5.0 \
                --output test/ns01/queue01 

# watch dir pega arquivos estruturados em diretorios enviando para queue test/ns01/queue01
./ssc-admin.py sources create \
                --name watch1 \
                --namespace test/ns01 \
                --classname Watchdogdir.Watchdogdir \
                --py ./builtin/sources/Watchdogdir/Watchdogdir.py \
                --configfile ./builtin/etc/watchdogdir_cfg.yaml \
                --timeout 5.0 \
                --output test/ns01/queue01 

Relay da queue test/ns01/queue03 para o redis
./ssc-admin.py sources create \
                --name source_relay_redis01 \
                --namespace test/ns01 \
                --classname SourceRedisQueue.SourceRedisQueue \
                --py ./builtin/sources/RedisQueue/SourceRedisQueue.py \
                --configfile ./builtin/etc/source_redis_queue.yaml \
                --timeout 5.0 \
                --output test/ns01/queue05


# list 
./ssc-admin.py sources list --namespace test/ns01

# -- Sinks --
# pega os dados da test/ns01/queue02 e os envia para um json em arquivo pelo TinyDB
./ssc-admin.py sinks create \
                --name tiny-teste \
                --namespace test/ns01 \
                --classname SinkTinydb.SinkTinydb \
                --py ./builtin/sinks/SinkTinydb/SinkTinydb.py \
                --configfile ./builtin/etc/sink_tinydb.yaml \
                --timeout 5.0 \
                --inputs test/ns01/queue02 

# pega os dados da test/ns01/queue02 e os envia para um csv em arquivo
./ssc-admin.py sinks create \
                --name csv-teste \
                --namespace test/ns01 \
                --classname SinkCSV.SinkCSV \
                --py ./builtin/sinks/SinkCSV/SinkCSV.py \
                --configfile ./builtin/etc/sink_csv.yaml \
                --timeout 5.0 \
                --inputs test/ns01/queue03 

# sink de gravacao de arquivos em diretorio
./ssc-admin.py sinks create \
                --name writer-test \
                --namespace test/ns01 \
                --classname SinkWriterFiles.SinkWriterFiles \
                --py ./builtin/sinks/SinkWriterFiles/SinkWriterFiles.py \
                --configfile ./builtin/etc/sink_writerfiles.yaml \
                --inputs test/ns01/queue04

# sink para redis queue
./ssc-admin.py sinks create \
                --name sink_redis_queue01 \
                --namespace test/ns01 \
                --classname SinkRedisQueue.SinkRedisQueue \
                --py ./builtin/sinks/SinkRedisQueue/SinkRedisQueue.py \
                --configfile ./builtin/etc/sink_redis_queue.yaml \
                --inputs test/ns01/queue02

# -- Functions --
# cria function para Relay da fila inputs test/ns01/queue01 para test/ns01/queue02
./ssc-admin.py functions create \
                --name relay01 \
                --namespace test/ns01 \
                --classname Relay.Relay \
                --py ./builtin/functions/Relay.py \
                --timeout 1.0 \
                --inputs test/ns01/queue01 \
                --output test/ns01/queue02

# Teste de client/producer
./ssc-client.py produce test/ns01/queue01 -m "teste 123..." -n 2 --key "0010201010" --properties "{\"val1\":\"aaa\"}"
./ssc-client.py consume -s appteste test/ns01/queue01 -n 1