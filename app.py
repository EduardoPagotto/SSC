'''
Created on 20220924
Update on 20220924
@author: Eduardo Pagotto
'''

from os import getenv
from flask import Flask

from SSC.server.DRegistry import DRegistry

# mypy: ignore-errors

SSC_CFG_IP : str  = getenv('SSC_CFG_IP') if getenv('SSC_CFG_IP') != None else '0.0.0.0' 
SSC_CFG_PORT : int  = int(getenv('SSC_CFG_PORT')) if getenv('SSC_CFG_PORT') != None else 5151
SSC_CFG_DB : str  = getenv('SSC_CFG_DB') if getenv('SSC_CFG_DB') != None else './data/db'
SSC_CFG_STORAGE : str = getenv('SSC_CFG_STORAGE') if getenv('SSC_CFG_STORAGE') != None else './data/storage'

rpc_registry = DRegistry(SSC_CFG_DB, SSC_CFG_STORAGE)

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = SSC_CFG_STORAGE
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024