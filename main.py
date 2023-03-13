#!/usr/bin/env python3
'''
Created on 20220924
Update on 20230313
@author: Eduardo Pagotto
'''

import json
from app import app, rpc_registry, SSC_CFG_IP, SSC_CFG_PORT 
from flask import request, jsonify

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

@app.route('/client-producer', methods=['POST'])
def client_producer():

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

@app.route('/client-subscribe', methods=['POST'])
def client_subscribe():

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

if __name__ == "__main__":
	app.run(host=SSC_CFG_IP, port=SSC_CFG_PORT)