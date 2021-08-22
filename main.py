from modules.DataGen import DataGen
import pyTigerGraph as tg
import paramiko
import sys
import requests 
from urllib.parse import urlparse

class DataGenException(Exception):
    def __init__(self, message):
        self.message = message
    
def login(advanced = False):
    if advanced:
        user_inp = {
            "host": input("host(http://127.0.0.1): "),
            "username": input("username(tigergraph): "),
            "password": input("password(tigergraph): "),
            "restppPort": input("restppPort(9000): "),
            "gsPort": input("gsPort(14240): "),
            "apiToken": input("host(None): "),
            "pem": input("pem(None): ")
        }
    else:
        user_inp = {
            "host": input("host(http://127.0.0.1): "),
            "sshPort": input("sshPort(22): "),
            "username": input("username(tigergraph): "),
            "password": input("password(tigergraph): "),
            "pem": input("pem_path(None): ")
        }

    print("...Connecting...")
    user_inp = {inp: user_inp[inp] for inp in user_inp if user_inp[inp] != ''}

    ssh_port = int(user_inp["sshPort"]) if "sshPort" in user_inp else 22
        
    pyTigerGraph_injectable = {"host": "http://127.0.0.1", "username": "tigergraph", "password": "tigergraph", "restppPort": "9000", "gsPort":"14240", "apiToken": ""}

    for key in pyTigerGraph_injectable:
        if key in user_inp:
            pyTigerGraph_injectable[key] = user_inp[key]

    try:
        conn = tg.TigerGraphConnection(**pyTigerGraph_injectable)     
    except Exception as e:
        raise DataGenException("Failed to connect to TG Instance")

    try:
        gs_ping_link = "%s/api/ping"%(conn.gsUrl)
        if (requests.get(gs_ping_link).json()["message"] != "pong"):
            raise Exception()
    except:
        raise DataGenException("Failed to connect to GS Endpoint @ %s"%(conn.gsUrl))
    
    try:
        conn.gsql("ls")
    except:
        raise DataGenException("Incorrect username/pw")

    try:
        restpp_ping_link = "%s/echo"%(conn.restppUrl)
        if (requests.get(restpp_ping_link).json()["message"] != "Hello GSQL"):
            raise Exception()
    except:
        raise DataGenException("Failed to connect to RESTPP Endpoint @ %s" %(conn.restppUrl))

    try:
        netloc = urlparse(pyTigerGraph_injectable["host"]).netloc
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        if "pem" in user_inp:
            k = paramiko.RSAKey.from_private_key_file(user_inp["pem"])
            ssh.connect(netloc, ssh_port, pyTigerGraph_injectable["username"], pyTigerGraph_injectable["password"], pkey=k)
        else:
            ssh.connect(netloc, ssh_port, pyTigerGraph_injectable["username"], pyTigerGraph_injectable["password"])

    except:
        raise DataGenException("Failed to connect to SSH endpoint @ %s:%s" %(netloc, ssh_port))

    return conn, ssh

def parse_simplified_schema(schema_def):
    simplified_schema = {}

    for vert in schema_def["VertexTypes"]:
        simplified_schema[vert["Name"]] = {"gsql_type": "VERTEX"}
    
    for edge in schema_def["EdgeTypes"]:
        simplified_schema[edge["Name"]] = {"gsql_type": "EDGE"}


    return simplified_schema

def get_datagen_config(schema):
    vert_config = {vert["Name"]: int(input("Enter quantity for node %s: "%vert["Name"]))  for vert in schema["VertexTypes"]}
    edge_config = {edge["Name"]: int(input("Enter quantity for edge %s: "%edge["Name"]))  for edge in schema["EdgeTypes"]}

    config = {
        "SEED": 1,
        "APPEND": True,
        "AUTO_FILL_VERTEX": "RANDOM",
        "AUTO_FILL_EDGE": "RANDOM",
        "NUM_VERTEX": vert_config,
        "NUM_EDGE": edge_config
    }

    return config

def main():
    is_advance = len(sys.argv) == 2 and sys.argv[1] == "advanced=true"
    conn, ssh = login(is_advance)
    conn.graphname = input("Enter graphname: ")
    schema = conn.getSchema()
    config = get_datagen_config(schema)
    datagen = DataGen(config, conn.graphname, parse_simplified_schema(schema), conn, ssh)

    print("Generating...")
    datagen.generate_and_store_data_on_TGInstance()

    print("Injecting...")
    script = datagen.get_loading_gen().generate_full_exec_script()
    print(conn.gsql(script))

main()


    