# SSC
Simple Stream Control

## Comando CRUD de filas 

[Criação topics](scripts/create_test.sh) : `./scripts/create_test.sh`

```bash
# lista filas em tenant test namespace ns01
./ssc-admin.py topics list test/ns01

# deleta fila queue03
./ssc-admin.py topics delete test/ns01/queue03

# lista namespaces em tenant test
./ssc-admin.py namespaces list test

# Lista tenants
./ssc-admin.py tenants list none

```

## Envio e recebimento as filas via CLI
```bash
# envai 5 mensagens a filas 1
./ssc-client.py produce test/ns01/queue01 -m "teste 123..." -n 5

# receber 2 mensagens de queue02
./ssc-client.py consume -s app1 test/ns01/queue02 -n 2

```

## Comandos CRUD para Sources de dados

[Criação Sources](scripts/create_test.sh) : `./scripts/create_test.sh`

```bash
# listar 
./ssc-admin.py sources list --tenant test --namespace ns01 --name none

# pause 
./ssc-admin.py sources pause --tenant test --namespace ns01 --name dummy-teste

# resume
./ssc-admin.py sources resume --tenant test --namespace ns01 --name dummy-teste

# remove 
./ssc-admin.py sources delete --tenant test --namespace ns01 --name dummy-teste

```

## Comandos CRUD de Functions

[Criação Functions](scripts/create_test.sh) : `./scripts/create_test.sh`

```bash
# listar 
./ssc-admin.py functions list --tenant test --namespace ns01 --name none

# pause 
./ssc-admin.py functions pause --tenant test --namespace ns01 --name name01

# resume 
./ssc-admin.py functions resume --tenant test --namespace ns01 --name name01

# remove 
./ssc-admin.py functions delete --tenant test --namespace ns01 --name name01

```

## Comandos CRUD de Sink de dados

[Criação Sinks](scripts/create_test.sh) : `./scripts/create_test.sh`

```bash
# listar 
./ssc-admin.py sinks list --tenant test --namespace ns01 --name none

# pause 
./ssc-admin.py sinks pause --tenant test --namespace ns01 --name tiny-teste

# resume
./ssc-admin.py sinks resume --tenant test --namespace ns01 --name tiny-teste

# remove 
./ssc-admin.py sinks delete --tenant test --namespace ns01 --name tiny-teste

```

## Running and debug local
1. Set VENV:
    ```bash
    # set env
    python3 -m venv .venv
    source .venv/bin/activate
    # install deps
    pip3 install -r requirements.txt
    ```

2. Start Server in line command
    ```bash
    cd ..
    ./main.py
    ```

## Service build, deply and test
1. Set VENV:
    ```bash
    # set env
    python3 -m venv .venv
    source .venv/bin/activate
    # install deps
    pip3 install -r requirements.txt
    ```

2. Build:
    ```bash
    # create ./dist/SSC.1.0.1.tar.gz
    make dist
    ```

3. Deploy
    ```bash
    cd deploy
    docker-compose up -d
    ```

4. Test local client
    ```bash
    cd ..
    ./client_rpc.py
    ```
    obs: No browser: http://127.0.0.1:5152 

## Maintenance of container
```bash
# Start the container and enter it for maintenance
docker run --name zdev -it SSC_server_img /bin/sh

# access the container working in interactive mode
docker exec -it server_SSC_dev /bin/sh
```

### TODO List
- [x] Implementar tenants
- [x] Testar tenants
- [x] Implementar namespaces
- [x] Testar namespaces
- [x] Implementar topics
- [x] Testar topics
- [x] Implementar plugin functions
- [x] Implementar pause/resume functions
- [x] Testar functions
- [x] Implementar payload com key, message_prop, timestamp
- [ ] Remover classes de producer e subscribe abstratas
- [ ] Implementar chamada de queue no RPC e limpeza de codigo
- [x] Implementar plugin sources (file / rest-api)
- [x] Implementar pause/resume sources
- [x] Testar sources
- [x] Implementar plugin sinks (file / rest-api)
- [x] Implementar pause/resume sinks
- [x] Testar sinks


refs: 
- https://roytuts.com/python-flask-rest-api-file-upload/

- https://code.visualstudio.com/docs/containers/quickstart-python

