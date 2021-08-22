from paramiko.client import SSHClient
from werkzeug.datastructures import is_immutable

class DataSourceException(Exception):
    def __init__(self, message, code = None):
        self.message = message
        self.code = code

class DataSource:

    def __init__(self, data_source = None, file_config = None):
        self.data_source = [] if data_source == None else data_source
        self.file_config = {} if file_config == None else file_config
        pass
    
    def add_ssh(self, ssh: SSHClient):
        self.ssh = ssh

    def check_data_source_params(self, link, has_header, separator):
        paths = self.get_all_paths(link)

        if paths == None or len(paths) == 0:
            return False, "Invalid path %s" % link
        
        if has_header not in ["y", "n"]:
            return False, "Invalid has_header"%has_header
        
        if len(separator) != 1:
            return False, "Invalid sepeartor"% separator
        
        return True, ""


    def add_data_source(self, link, has_header, separator):
        paths = self.get_all_paths(link)
        
        if paths == None:
            raise DataSourceException("invalid source", 400)
        
        paths = [path for path in paths if self.is_valid_source(path)]

        if len(paths) == 0:
            raise DataSourceException("invalid source", 400)
            
        if has_header != "y" and has_header != "n":
            raise DataSourceException("invalid has_header", 400)
        
        if len(separator) != 1:
            raise DataSourceException("invalid separator", 400)

        count = 0
        added_files = []
        for source in paths:
            flag = True in [ds["link"] == source for ds in self.data_source]

            if flag:
                continue

            count += 1
            added_files.append(source)

            self.data_source.append({"link": source, "has_header": True if has_header == "y" else False, "separator": separator})
        
        return "%i files added: %s" % (count, ", ".join([path.split("/")[-1] for path in added_files]))


    def generate_file_def(self):
        if (len(self.data_source) == 0):
            raise DataSourceException("No file source found", 400)
            
        file_config = {}
        curr_index = 0

        for source in self.data_source:
            alias = "f"+str(curr_index)
            file_config[alias] = {"link": source["link"], "header": "\"%s\""%("true" if source["has_header"] else "false"), "separator": "\"%s\""%(source["separator"])}

            curr_index += 1

        self.file_config = file_config

        return file_config

    def get_description(self):
       return "\n###########################\n".join(["ALIAS:    %s\nFILE:     %s\nHEADERS:  %s"%(alias, self.file_config[alias]["link"], self.get_headers(alias)) for alias in self.file_config])

    def is_valid_source(self, source):
        _, stdout, _ = self.ssh.exec_command("[ -f %s ] && echo \"True\" || echo \"False\""%(source))
        result = stdout.readlines()[0].strip() == "True"

        return result
    
    def get_all_paths(self, source):
        _, stdout, _ = self.ssh.exec_command("cd /; ls %s" % source)
        paths = []
        results = [result.strip() for result in stdout.readlines()]

        for result in results:
            if "No such file or directory" in result:
                return None
            
            paths.append(result)
        
        return results

    def get_headers(self, alias, include_header_name = True):
        _, stdout, _ = self.ssh.exec_command("head -1 %s"%(self.file_config[alias]["link"]))
        first_line_heads = stdout.readlines()[0].strip().split(",")

        if include_header_name and self.file_config[alias]["header"] == "\"true\"":
            return ", ".join(["$%s(%s)"%(str(i), first_line_heads[i]) for i in range(0,len(first_line_heads))]) 

        return ", ".join(["$%s"%(str(i)) for i in range(0, len(first_line_heads))])
    
    def get_file_headers_dict(self):
        return {alias: (self.file_config[alias]["link"], self.get_headers(alias)) for alias in self.file_config}

