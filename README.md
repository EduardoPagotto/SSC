# SSC
Simple Stream Control

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

3. Start test client
    ```bash
    # In other terminal im projet directory
    ./client_rpc.py
    ```
    obs: No browser: http://127.0.0.1:5151 


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
    obs: No browser: http://127.0.0.1:5151 

## Maintenance of container
```bash
# Start the container and enter it for maintenance
docker run --name zdev -it SSC_server_img /bin/sh

# access the container working in interactive mode
docker exec -it server_SSC_dev /bin/sh
```

refs: 
- https://roytuts.com/python-flask-rest-api-file-upload/