# SSC
Simple Stream Control

## Comandos 
```bash
# Cria tenant/namespace/topic 
./ssc-admin.py tenants create test
./ssc-admin.py namespaces create test/ns01
./ssc-admin.py topics create test/ns01/queue01
./ssc-admin.py topics create test/ns01/queue02

# cria funcs
./ssc-admin.py functions \
                create \
                --name name01 \
                --tenant test \
                --namespace ns01 \
                --py ./src/FuncAdd.py \
                --classname FuncAdd.FuncAdd \
                --inputs test/ns01/queue01 \
                --output test/ns01/queue02


# envai dados de teste a filas 1
./ssc-client.py produce test/ns01/queue01 -m "teste 123..." -n 5

# FuncAdd le queue01 e envia a queue02!!

# receber dados em queue02
./ssc-client.py consume -s app1 test/ns01/queue02 -n 2

# remove a func named01
./ssc-admin.py functions delete --tenant test --namespace ns01 --name name01


./ssc-admin.py functions list --tenant test --namespace ns01 --name none
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

refs: 
- https://roytuts.com/python-flask-rest-api-file-upload/