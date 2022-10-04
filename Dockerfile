FROM python:3.10.7-alpine3.16 
LABEL MAINTAINER="Eduardo Pagotto <eduardo.pagotto@newspace.com.br>"

# install dep of RPC
WORKDIR /var/app/sJsonRpc
ADD ./sJsonRpc/sJsonRpc ./sJsonRpc
COPY ./sJsonRpc/requirements.txt .
COPY ./sJsonRpc/setup.py .

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt && \
    pip3 install . 

WORKDIR /var/app

COPY ./requirements.txt .
COPY ./app.py .
COPY ./main.py .
COPY ./setup.py .
COPY ./ssc-admin.py .
COPY ./ssc-client.py .
ADD ./SSC /var/app/SSC/.

RUN pip3 install -r requirements.txt && \
    pip3 install .

ENV SSC_CFG_IP "0.0.0.0"
ENV SSC_CFG_PORT "5152"
ENV SSC_CFG_DB "/opt/db"
ENV SSC_CFG_STORAGE "/opt/storage"

EXPOSE 5152/tcp

# Iniciar aplicação
CMD ["python3", "./main.py"]