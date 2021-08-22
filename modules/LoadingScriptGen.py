import re

class LoadingException(Exception):
    def __init__(self, message, code = None):
        self.message = message
        self.code = code

class LoadingScriptGen():
    def __init__(self, file_config, receiver_injector_dict, schema_def, pack_name, is_infer = True):
        self.schema_def = schema_def
        self.pack_name = pack_name
        self.file_config = file_config      
        self.receiver_injector_dict = receiver_injector_dict
        self.is_infer = is_infer

    def generate_full_exec_script(self):
        return "USE GRAPH %s\nCREATE LOADING JOB tg_pack_loading_%s FOR GRAPH %s {\n%s;\n}\nRUN LOADING JOB tg_pack_loading_%s\nDROP JOB tg_pack_loading_%s" \
                %(self.pack_name, self.pack_name, self.pack_name, ";\n".join(self.generate_script()), self.pack_name, self.pack_name)

    def generate_script(self): 
        gsql_file_def_script = ["DEFINE FILENAME %s = \"%s\""%(alias, self.file_config[alias]["link"]) for alias in self.file_config]
        try:
            loading_scripts = [self.parse_loading(receiver, self.schema_def[receiver]["gsql_type"], injector) for receiver in self.receiver_injector_dict for injector in self.receiver_injector_dict[receiver]]
        except:
            raise LoadingException("Incorrect file name or header identifier", 400)
        
        if (self.is_infer):
            try:
                loading_scripts += self.get_loading_script_for_edges_of_feature_vertices()
            except:
                raise LoadingException("Unable to find key for at least one hub node", 400)
                
        return gsql_file_def_script + loading_scripts
    
    def get_loading_script_for_edges_of_feature_vertices(self):
        loading_scripts = []

        pk_source_dict = {}
        for receiver in self.receiver_injector_dict:
            pk_source_dict[receiver] = []
            for injector in self.receiver_injector_dict[receiver]:
                file_alias, headers = injector
                pk_source_dict[receiver].append("%s.%s"%(file_alias, headers.split(",")[0]))
        

        for receiver in self.receiver_injector_dict:
            if receiver in self.schema_def and "is_feature_vertex" in self.schema_def[receiver] and self.schema_def[receiver]["is_feature_vertex"]:
                feature_pk_source = pk_source_dict[receiver]
                entity_pk_source = pk_source_dict[self.schema_def[receiver]["connected_to"][0][0]]
                connected_edge = self.schema_def[receiver]["connected_to"][0][1]

                loading_scripts += self.get_linking_script(feature_pk_source, entity_pk_source, connected_edge)
        
        return loading_scripts

    def get_linking_script(self, f_pk_s, e_pk_s, edge):
        scripts = []

        for f_pk in f_pk_s:
            f_file, f_header = f_pk.split(".")
            e_file = f_file
            e_header = ""

            for e_pk in e_pk_s:
                if e_pk.split(".")[0] == e_file:
                    e_header = e_pk.split(".")[1]
                    break
            
            if (e_header != ""):
                scripts.append("LOAD %s TO EDGE %s VALUES(%s, %s) USING header = %s, separator = %s" %(f_file, edge, e_header, f_header, self.file_config[f_file]["header"], self.file_config[f_file]["separator"]))
            else:
                raise Exception()

        return scripts

    def parse_loading(self, receiver, receiver_type, injector):
        injector_alias, header_gsql_syntax = injector
        
        return "LOAD %s TO %s %s VALUES(%s) USING header = %s, separator = %s" % (injector_alias, receiver_type, receiver, header_gsql_syntax, self.file_config[injector_alias]["header"], self.file_config[injector_alias]["separator"])
    