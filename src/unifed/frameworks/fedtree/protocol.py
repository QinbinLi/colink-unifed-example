import os
import json
import sys
import subprocess
import tempfile
import logging
from typing import List

import colink as CL

from unifed.frameworks.fedtree.util import store_error, store_return, get_local_ip

pop = CL.ProtocolOperator(__name__)
UNIFED_TASK_DIR = "unifed:task"

def load_config_from_param_and_check(param: bytes):
    unifed_config = json.loads(param.decode())
    framework = unifed_config["framework"]
    assert framework == "fedtree"
    deployment = unifed_config["deployment"]
    if deployment["mode"] != "colink":
        raise ValueError("Deployment mode must be colink")
    return unifed_config

@pop.handle("unifed.fedtree:server")
@store_error(UNIFED_TASK_DIR)
@store_return(UNIFED_TASK_DIR)
def run_server(cl: CL.CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info("start server")
    server_ip = get_local_ip()
    cl.send_variable("server_ip", server_ip, [p for p in participants if p.role == "client"])
    unifed_config = load_config_from_param_and_check(param)
    tree_param = unifed_config['training']['tree_param']
    n_clients = len(participants) - 1
    with open('fedtree_server.conf', mode='w') as f:
        f.write(f"n_parties={n_clients}\n")
        f.write(f"n_trees={tree_param['n_trees']}\n")
        f.write(f"depth={tree_param['max_depth']}\n")
        f.write(f"max_num_bin={tree_param['max_num_bin']}\n")
        if unifed_config['algorithm'] == 'histsecagg':
            f.write(f"mode=horizontal\n")
            f.write(f"privacy_tech=sa\n")
        elif unifed_config['algorithm'] == 'secureboost':
            f.write(f"mode=vertical\n")
            f.write(f"privacy_tech=he\n")
            f.write(f"data=./data/{unifed_config['dataset']}"+"_1.csv\n")
        f.write(f"data_format=csv\n")
        f.write(f"objective={tree_param['objective']}\n")
        f.write(f"learning_rate={tree_param['learning_rate']}\n")
    os.system(
        f"./FedTree/build/bin/FedTree-distributed-server fedtree_server.conf")
    
@pop.handle("unifed.fedtree:client")
@store_error(UNIFED_TASK_DIR)
@store_return(UNIFED_TASK_DIR)
def run_client(cl: CL.CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info("start client")
    unifed_config = load_config_from_param_and_check(param)
    tree_param = unifed_config['training']['tree_param']
    server_in_list = [p for p in participants if p.role == "server"]
    assert len(server_in_list) == 1
    p_server = server_in_list[0]
    server_ip = cl.recv_variable("server_ip", p_server).decode()

    n_clients = len(participants) - 1
    user_id = cl.get_participant_index(participants)
    with open('fedtree_client' + str(user_id) + '.conf', mode='w') as f:
        f.write(f"n_parties={n_clients}\n")
        f.write(f"n_trees={tree_param['n_trees']}\n")
        f.write(f"depth={tree_param['max_depth']}\n")
        f.write(f"max_num_bin={tree_param['max_num_bin']}\n")
        if unifed_config['algorithm'] == 'histsecagg':
            f.write(f"mode=horizontal\n")
            f.write(f"privacy_tech=sa\n")
            f.write(f"n_features={unifed_config['n_features']}\n")
        elif unifed_config['algorithm'] == 'secureboost':
            f.write(f"mode=vertical\n")
            f.write(f"privacy_tech=he\n")
        f.write(f"data_format=csv\n")
        f.write(f"objective={tree_param['objective']}\n")
        f.write(f"learning_rate={tree_param['learning_rate']}\n")
        f.write(f"data=./data/{unifed_config['dataset']}"+"_"+str(user_id)+".csv\n")
        f.write(f"ip_address={server_ip}\n")
        test_data_path = f"./data/{unifed_config['dataset']}"+"_test.csv"
        if os.path.isfile(f"{test_data_path}"):
            f.write(f"test_data={test_data_path}\n")
    os.system("./FedTree/build/bin/FedTree-distributed-party fedtree_client" + str(user_id) + ".conf " + str(user_id-1))
    #TODO: write logs
    cl.create_entry("task:{}:output".format(cl.get_task_id()), "finished")
