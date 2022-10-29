#!/usr/bin/env python3
'''
Created on 20220924
Update on 20221029
@author: Eduardo Pagotto
'''

import json
import os
import pathlib
from app import app, rpc_registry, SSC_CFG_IP, SSC_CFG_PORT 
from flask import request, jsonify, send_from_directory

ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'zip', 'jpg'])

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/")
def home():
	resp = jsonify(rpc_registry.sumario())
	resp.status_code = 201
	return resp

@app.route('/client-queue', methods=['POST'])
def client_queue():

	try :
		input_rpc : dict = json.loads(request.headers.get('rpc-Json'))
		output_rpc = rpc_registry.call(input_rpc)
		resp = jsonify(output_rpc)
		resp.status_code = 201
		return resp

	except Exception as exp:
		msg = exp.args[0]
		rpc_registry.log.error(msg)
		resp = jsonify({'message' : msg})
		resp.status_code = 400
		return resp

@app.route('/admin', methods=['POST'])
def client_admin():

	try :
		input_rpc : dict = json.loads(request.headers.get('rpc-Json'))
		output_rpc = rpc_registry.call(input_rpc)
		resp = jsonify(output_rpc)
		resp.status_code = 201
		return resp

	except Exception as exp:
		msg = exp.args[0]
		rpc_registry.log.error(msg)
		resp = jsonify({'message' : msg})
		resp.status_code = 400
		return resp

# @app.route('/rpc-call-upload', methods=['POST'])
# def rpc_call_upload():

# 	try :
# 		input_rpc : dict = json.loads(request.headers.get('rpc-Json'))
# 		if 'file' in request.files:
# 			input_rpc['params'].append(request.files['file'])
# 		else:
# 			input_rpc['params'].append(None)

# 		output_rpc = rpc_registry.call(input_rpc)
# 		resp = jsonify(output_rpc)
# 		resp.status_code = 201
# 		return resp

# 	except Exception as exp:
# 		msg = exp.args[0]
# 		rpc_registry.log.error(msg)
# 		resp = jsonify({'message' : msg})
# 		resp.status_code = 400
# 		return resp


# @app.route('/download/<path:path>',methods = ['GET','POST'])
# def rpc_call_download(path):
# 	"""Download a file."""
# 	try:
# 		data : dict = rpc_registry.infoAll(int(path))
# 		path_all =  pathlib.Path(data['internal'])
# 		uploads = os.path.join(app.config.root_path, path_all.parent)
# 		rpc_registry.tot_dowload += 1
# 		return send_from_directory(uploads, path_all.name, as_attachment=True)

# 	except Exception as exp:
# 		msg = exp.args[0]
# 		rpc_registry.log.error(msg)
# 		resp = jsonify({'message' : msg})
# 		resp.status_code = 400
# 		return resp	

if __name__ == "__main__":
	app.run(host=SSC_CFG_IP, port=SSC_CFG_PORT)