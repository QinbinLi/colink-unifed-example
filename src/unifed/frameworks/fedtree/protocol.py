import os
import json
import subprocess
import tempfile
import logging
from typing import List

import colink as CL

from unifed.frameworks.fedtree.util import store_error, store_return, get_local_ip

pop = CL.ProtocolOperator(__name__)
UNIFED_TASK_DIR = "unifed:task"


USE_FILE_LOGGING = False
if USE_FILE_LOGGING:
    logging.basicConfig(filename="temp.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)


def load_config_from_param_and_check(param: bytes):
    unifed_config = json.loads(param.decode())
    framework = unifed_config["framework"]
    assert framework == "fedtree"
    deployment = unifed_config["deployment"]
    if deployment["mode"] != "colink":
        raise ValueError("Deployment mode must be colink")
    return unifed_config


def convert_unifed_config_to_fedtree_config(unifed_config):  # note that for the target config, the "data" field is still missing
    tree_param = unifed_config['training']['tree_param']
    fedtree_config = {
        "n_parties": len(unifed_config["deployment"]["participants"]) - 1,  # fedTree does not count the server in "n_parties"
        "n_trees": tree_param['n_trees'],
        "depth": tree_param['max_depth'],
        "max_num_bin": tree_param['max_num_bin'],
        "data_format": "csv",
        "objective": tree_param['objective'],
        "learning_rate": tree_param['learning_rate'],
        "verbose": 1,
    }
    if unifed_config['algorithm'] == 'histsecagg':
        fedtree_config["mode"] = "horizontal"
        fedtree_config["privacy_tech"] = "sa"
    elif unifed_config['algorithm'] == 'secureboost':
        fedtree_config["mode"] = "vertical"
        fedtree_config["privacy_tech"] = "he"
    else:
        raise ValueError(f"Unknown algorithm {unifed_config['algorithm']}")
    return fedtree_config


def create_conf_file_content_from_fedtree_config(fedtree_config):
    conf_file_content = ""
    for key, value in fedtree_config.items():
        conf_file_content += f"{key}={value}\n"
    return conf_file_content


def filter_log_from_fedtree_output(output):
    filtered_output = ""
    for line in output.split("\n"):
        line_json = json.dumps({"fedtree": line})  # TODO: convert log here
        if ".cpp" in line:
            filtered_output += f"{line_json}\n"
    return filtered_output


@pop.handle("unifed.fedtree:server")
@store_error(UNIFED_TASK_DIR)
@store_return(UNIFED_TASK_DIR)
def run_server(cl: CL.CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info(f"{cl.get_task_id()[:6]} - Server - Started")
    unifed_config = load_config_from_param_and_check(param)
    fedtree_config = convert_unifed_config_to_fedtree_config(unifed_config)
    if unifed_config['algorithm'] == 'secureboost':
        fedtree_config["data"] = f"./data/{unifed_config['dataset']}_1.csv"
    temp_conf_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    temp_conf_file.write(create_conf_file_content_from_fedtree_config(fedtree_config))
    temp_conf_file.close()
    process = subprocess.Popen(
        [
            "./bin/FedTree-distributed-server",
            # takes 1 arg: config file path
            temp_conf_file.name,
        ],
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
    )
    # make sure the server is started first before sharing IP
    server_ip = get_local_ip()
    cl.send_variable("server_ip", server_ip, [p for p in participants if p.role == "client"])
    # as soon as one client finishes, all clients should have finished
    first_client_return_code = cl.recv_variable("client_finished", [p for p in participants if p.role == "client"][0])
    process.kill()
    stdout, stderr = process.communicate()
    log = filter_log_from_fedtree_output(stdout.decode() + stderr.decode())
    # TODO: store the model
    cl.create_entry(f"{UNIFED_TASK_DIR}:{cl.get_task_id()}:log", log)
    os.unlink(temp_conf_file.name)
    return json.dumps({
        "server_ip": server_ip,
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
        "returncode": CL.byte_to_int(first_client_return_code),
    })
    
    
@pop.handle("unifed.fedtree:client")
@store_error(UNIFED_TASK_DIR)
@store_return(UNIFED_TASK_DIR)
def run_client(cl: CL.CoLink, param: bytes, participants: List[CL.Participant]):
    logging.info(f"{cl.get_task_id()[:6]} - Client[(unknown)] - Started")
    server_in_list = [p for p in participants if p.role == "server"]
    assert len(server_in_list) == 1, f"There should be exactly one server, not {len(server_in_list)} servers ({server_in_list})."
    p_server = server_in_list[0]
    def get_client_id():
        passed_server = 0
        for i, p in enumerate(participants):
            if p.role == "server":
                passed_server += 1
            if p.user_id == cl.get_user_id():
                return i - passed_server
    client_id = get_client_id()
    logging.info(f"{cl.get_task_id()[:6]} - Client[{client_id}] - Recognized")

    unifed_config = load_config_from_param_and_check(param)
    fedtree_config = convert_unifed_config_to_fedtree_config(unifed_config)
    fedtree_config["data"] = f"./data/{unifed_config['dataset']}_{client_id}.csv"
    if unifed_config['algorithm'] == 'histsecagg':
        fedtree_config["n_features"] = {"breast_horizontal": 30}[unifed_config['dataset']]  # currently just a hack, later we should get this from the mapping file for predefined datasets
    test_data_path = f"./data/{unifed_config['dataset']}"+"_test.csv"
    if os.path.isfile(f"{test_data_path}"):
        fedtree_config["test_data"] = test_data_path
    server_ip = fedtree_config["ip_address"] = cl.recv_variable("server_ip", p_server).decode()

    temp_conf_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    temp_conf_file.write(create_conf_file_content_from_fedtree_config(fedtree_config))
    temp_conf_file.close()
    process = subprocess.Popen(
        [
            "./bin/FedTree-distributed-party",
            # takes 2 arg: config file path, client id
            temp_conf_file.name,
            str(client_id),
        ],
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    returncode = process.returncode
    logging.info(f"{cl.get_task_id()[:6]} - Client[{client_id}] - Subprocess finished with return code {returncode}")
    # as soon as one client finishes, all clients should have finished
    if client_id == 0:
        cl.send_variable("client_finished", returncode, server_in_list)
    log = filter_log_from_fedtree_output(stdout.decode() + stderr.decode())
    cl.create_entry(f"{UNIFED_TASK_DIR}:{cl.get_task_id()}:log", log)
    os.unlink(temp_conf_file.name)
    return json.dumps({
        "server_ip": server_ip,
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
        "returncode": returncode,
    })
