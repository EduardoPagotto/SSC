'''
Created on 20220924
Update on 20221109
@author: Eduardo Pagotto
'''

import logging
from os import getenv
import pathlib
from flask import Flask

from tinydb import TinyDB

from SSC.server.DRegistry import DRegistry
from SSC.server.FunctionCrt import FunctionCrt
from SSC.server.SourceCrt import SourceCrt
from SSC.server.Tenant import Tenant

from SSC.__init__ import __version__ as VERSION
from SSC.__init__ import __date_deploy__ as DEPLOY

# mypy: ignore-errors

SSC_CFG_IP : str  = '0.0.0.0' if getenv('SSC_CFG_IP') is None else getenv('SSC_CFG_IP')
SSC_CFG_PORT : int  =  5152 if getenv('SSC_CFG_PORT') is None else int(getenv('SSC_CFG_PORT'))
SSC_CFG_DB : str  = './data/db' if getenv('SSC_CFG_DB') is None else getenv('SSC_CFG_DB')
SSC_CFG_STORAGE : str = './data/storage' if getenv('SSC_CFG_STORAGE') is None else getenv('SSC_CFG_STORAGE')
#REDIS_URL : str = 'redis://localhost:6379/0' if getenv('REDIS_URL') is None else getenv('REDIS_URL') 
REDIS_URL : str = 'redis://:AAABBBCCC@192.168.122.1:6379/0' if getenv('REDIS_URL') is None else getenv('REDIS_URL')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(threadName)-16s %(funcName)-20s %(message)s',
    datefmt='%H:%M:%S',
)

logging.getLogger('werkzeug').setLevel(logging.CRITICAL) #urllib3
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

log = logging.getLogger('SSC')
log.info(f'>>>>>> SSC v-{VERSION} ({DEPLOY})')

path1 = pathlib.Path(SSC_CFG_DB)
path1.mkdir(parents=True, exist_ok=True)

path2 = pathlib.Path(SSC_CFG_STORAGE)
path2.mkdir(parents=True, exist_ok=True)

database = TinyDB(str(path1) + '/master.json')
rpc_registry = DRegistry(FunctionCrt(database, SSC_CFG_STORAGE), SourceCrt(database, SSC_CFG_STORAGE), Tenant(database, SSC_CFG_STORAGE, REDIS_URL))

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = SSC_CFG_STORAGE
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024