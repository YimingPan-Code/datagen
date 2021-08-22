from urllib.parse import urlparse
from . import DataGenerator 
import os 
import json
from .DataSource import DataSource
from .LoadingScriptGen import LoadingScriptGen

class DataGen:
    
    def __init__(self, config_dict, graphname, schema_def, tg_conn, ssh):
        self.config_dict = config_dict
        self.graphname = graphname
        self.temp_directory = "tp-datagen"
        self.abs_ssh_path = None
        self.tg_conn = tg_conn
        self.ssh = ssh
        self.schema_def = schema_def
        self.sftp = ssh.open_sftp()

    def generate_and_store_data_on_TGInstance(self):
        _, stdout, _ = self.ssh.exec_command("rm -r %s; mkdir %s"%(self.temp_directory, self.temp_directory))
        stdout.readlines()


        _, stdout, _ = self.ssh.exec_command("pwd")
        self.abs_ssh_path = stdout.readlines()[0].strip()

        generator_path = os.path.join(os.path.abspath(os.path.dirname(DataGenerator.__file__)), "gsql_data_generator")
        config_path = os.path.join(os.path.abspath(os.path.dirname(DataGenerator.__file__)), "config.json")

        with open(config_path, 'w') as f:
            json.dump(self.config_dict, f, indent = 2)

        generator_ssh_path = "%s/%s/gsql_data_generator"%(self.abs_ssh_path, self.temp_directory)
        config_ssh_path = "%s/%s/config.json"%(self.abs_ssh_path, self.temp_directory)
        schema_ssh_path = "%s/%s/schema.yaml"%(self.abs_ssh_path, self.temp_directory)

        self.sftp.put(generator_path, generator_ssh_path)
        self.sftp.put(config_path, config_ssh_path)
        self.tg_conn.gsql("USE GRAPH %s\nexport schema %s to \"%s\""%(self.graphname, self.graphname, schema_ssh_path))

        _, stdout, _ = self.ssh.exec_command("cd %s; chmod +x ./gsql_data_generator; ./gsql_data_generator schema.yaml config.json" % self.temp_directory)
        stdout.readlines()

    def get_folder_path(self):
        return "%s/%s"%(self.abs_ssh_path, self.temp_directory)
    
    def get_loading_gen(self):

        datasource = DataSource()
        datasource.add_ssh(self.ssh)
        datasource.add_data_source("%s/*.csv"%self.get_folder_path(), "y", ",")
        
        file_def = datasource.generate_file_def()
        receiver_injector_dict = {file_def[alias]["link"].split("/")[-1].split(".")[0]: self.parse_injector("%s(%s)"%(alias, datasource.get_headers(alias, False))) for alias in file_def}

        return LoadingScriptGen(file_def, receiver_injector_dict, self.schema_def, self.graphname, is_infer = False)

    def get_datasource_and_mapping_config(self):
        datasource = DataSource()
        datasource.add_ssh(self.ssh)
        datasource.add_data_source("%s/*.csv"%self.get_folder_path(), "y", ",")
        file_def = datasource.generate_file_def()
        receiver_injector_dict = {file_def[alias]["link"].split("/")[-1].split(".")[0]: self.parse_injector("%s(%s)"%(alias, datasource.get_headers(alias, False))) for alias in file_def}

        return datasource, receiver_injector_dict

    def parse_injector(self, injector_inp):
        import re
        pattern = r"[a-z0-9A-Z_]+\(\$[0-9]+( *, *\$[0-9]+)*\)( *; *[a-z0-9A-Z_]+\(\$[0-9]+( *, *\$[0-9]+)*\))*"

        if re.fullmatch(pattern, injector_inp) == None:
            return None
        
        splitted_by_file = injector_inp.split(";")
        
        return_arr = []

        for file_injector in splitted_by_file:
            headers = re.search(r'(?<=\().*(?=\))', file_injector).group()
            file_name = file_injector.replace("(%s)"%headers, '')
            
            return_arr.append((file_name.strip(), headers.strip()))

        return return_arr
    
