'''
Created on 20220924
Update on 20220924
@author: Eduardo Pagotto
'''

from os import getenv
from flask import Flask

from SSC.server.DRegistry import DRegistry
from SSC.server.DataBaseCrt import DataBaseCrt
from SSC.server.FunctionCrt import FunctionCrt
from SSC.server.FunctionDB import FunctionDB
from SSC.server.NameSpace import NameSpace
from SSC.server.Tenant import Tenant
from SSC.server.TopicCrt import TopicsCrt
from SSC.server.TopicDB import TopicDB

# mypy: ignore-errors

SSC_CFG_IP : str  = getenv('SSC_CFG_IP') if getenv('SSC_CFG_IP') != None else '0.0.0.0' 
SSC_CFG_PORT : int  = int(getenv('SSC_CFG_PORT')) if getenv('SSC_CFG_PORT') != None else 5152
SSC_CFG_DB : str  = getenv('SSC_CFG_DB') if getenv('SSC_CFG_DB') != None else './data/db'
SSC_CFG_STORAGE : str = getenv('SSC_CFG_STORAGE') if getenv('SSC_CFG_STORAGE') != None else './data/storage'

database_crt = DataBaseCrt(SSC_CFG_DB)

topic_db = TopicDB(database_crt)
topic_crt = TopicsCrt(topic_db)

function_db = FunctionDB(database_crt, topic_crt)
function_crt = FunctionCrt(function_db, SSC_CFG_STORAGE)

tenant = Tenant(SSC_CFG_STORAGE)
namespace = NameSpace(tenant)

rpc_registry = DRegistry(topic_crt, function_crt, tenant, namespace)

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = SSC_CFG_STORAGE
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024