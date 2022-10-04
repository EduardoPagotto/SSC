# SSC
Simple Stream Control

## Comandos 
```bash
# Cria fila 
./ssc-admin.py topics create queue01
./ssc-admin.py topics create queue02
#./ssc-admin.py topics create fila1
#./ssc-admin.py topics delete fila1

# cria funcs
./ssc-admin.py functions \
                create \
                --name name01 \
                --py ./funcs/FuncAdd.py \
                --classname externo.FuncAdd.FuncAdd \
                --inputs queue01 \
                --output queue02

# envai dados de teste a filas 1
./ssc-client.py produce queue01 -m "entrando em 1" -n 10

# FuncAdd le queue01 e envia a queue02!!
# receber dados em queue02
./ssc-client.py consume -s appteste queue02 -n 10

# remove a func named01
./ssc-admin.py functions delete --name name01

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