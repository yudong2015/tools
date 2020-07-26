#!/usr/bin/env python

import requests
import logging

ROLE_CTL = "controller"
ROLE_CMP = "node"
ROLE_LOGIN = "login"

HOST_URL = "http://metadata/self/host/{}"
HOSTS_URL = "http://metadata/self/hosts/{}"

IP_KEY = "/{}/ip "
SID_KEY = "/{}/sid "


IP_FORMAT = "{}    {}{}"  # eg: 192.168.0.10    node10

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)')
logger = logging.getLogger(__name__)


def my_role():
    url = HOST_URL.format("role")
    return request_metadata(url, ret_json=False)


def request_metadata(url, ret_json=True):
    res = requests.get(url)
    if res.status_code != 200:
        logger.error("Failed to request[{}] controller: [{}] {}".format(url, res.status_code, res.text))
        exit(1)
    if ret_json:
        return res.json()
    return res.text


def get_role_nodes(role):
    # {
    #   "instance_id": {
    #     "ip": xxxxx,
    #     "sid": xxxxx
    #   }
    # }
    instances = {}
    content = request_metadata(HOSTS_URL.format(role))
    for item in content:
        i_id = item.split("/")[1]
        instance = instances.get(i_id, {})
        if item.startswith(IP_KEY.format(i_id)):
            instance["ip"] = item.lstrip(IP_KEY.format(i_id)).strip(' ')
        elif item.startswith(SID_KEY.format(i_id)):
            instance["sid"] = item.lstrip(SID_KEY.format(i_id)).strip(' ')
        else:
            continue
        instances[i_id] = instance
    logger.info("Get node info of [{}]: {}".format(role, instances))
    return instances


def get_single_node_ip(role):
    instances = get_role_nodes(role)
    ip = ""
    if instances:
        key = instances.keys()[0]
        ip = instances[key].get("ip", "")

    if ip:
        return ip
    else:
        logger.error("IP of {} not exist in {}!".format(role, instances))
        exit(1)


def generate():
    role = my_role()
    ctl_ip = get_single_node_ip()
    with open("/etc/hosts", "a+") as f:
        f.write("# controller host")
        f.write(IP_FORMAT.format(ctl_ip, ROLE_CTL, ""))

        if role == ROLE_LOGIN:
            login_node_ip = get_single_node_ip(ROLE_LOGIN)
            f.write(IP_FORMAT.format(login_node_ip, ROLE_LOGIN, ""))
        else:
            node_instances = get_role_nodes(ROLE_CMP)
            for instance in node_instances:
                f.write(IP_FORMAT.format(instance["ip"], ROLE_CMP, instance["sid"]))
    logger.info("Generate hosts for {} successfully.".format(role))

