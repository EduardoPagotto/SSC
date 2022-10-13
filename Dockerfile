FROM python:3.10.7-alpine3.16 
LABEL MAINTAINER="Eduardo Pagotto <eduardo.pagotto@newspace.com.br>"

RUN apk update
RUN apk add git

RUN pip3 install --upgrade pip

WORKDIR /var/app

#lib deps
RUN git clone https://github.com/EduardoPagotto/sJsonRpc.git
WORKDIR /var/app/sJsonRpc/
RUN pip3 install -r requirements.txt && \
    pip3 install .
WORKDIR /var/app
RUN rm -rf sJsonRpc

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
ENV SSC_CFG_DB "./data/db"
ENV SSC_CFG_STORAGE "./data/storage"

EXPOSE 5152/tcp

# Iniciar aplicação
CMD ["python", "main.py"]
