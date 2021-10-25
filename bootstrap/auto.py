#!/usr/bin/env python
import sys
import argparse
import os
import subprocess
import argparse
import json
from typing import Dict

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509.oid import NameOID, ObjectIdentifier

def process_cmd_opts():
    def interpret_help() -> None:
        set = True if "-h" in sys.argv else False
        if set:
            if (len(sys.argv)-1 < 2):
                print("usage: ndncert-dummy-client -c SSL_CERT -p SSL_PRV")
                print("                           [-h] [-i CA_INDEX] [-n IDENTITY_NAME] [-v VALIDITY]")
                print("                           [-g CONFIG_FILE]                                   ")
                print("       ndncert-dummy-client: dummy ndncert client customized for Hydra project.")
                print("")
                print("* informational args:")
                print("  -h, --help                       |   shows this help message and exits.")
                print("")
                print("* required args:")
                print("  -c, --ssl_cert SSL_CERT          |   SSL Cert in PEM format for authentication. Example: \"bruins_cert.crt\"")
                print("  -p, --ssl_prv SSL_PRV            |   Private key of SSL cert. Example: \"bruins_prv.pem\"")
                print("  -i, --ca_index CA_INDEX          |   CA Index. Normally 0. Example: \"0\"")
                print("  -n, --id_name ID_NAME            |   Identity name. Example: \"/edu/ucla/cs/bruins/hydra_op\"")
                print("  -v, --validity VALIDITY          |   Cert Validaty in hours (CA may reject too long validity). Example: \"100\"")
                print("  -g, --config_file CONFIG_FILE    |   Client configuration File.  Example: \"client.conf\"")
                print("")
                print("Thank you for using ndncert-dummy-client.")
            sys.exit(0)

    def parse_cmd_opts():
        # Command Line Parser
        parser = argparse.ArgumentParser(prog="ndncert-dummy-client",add_help=False,allow_abbrev=False)

        # Adding all Command Line Arguments
        default_client_conf= "/usr/local/etc/ndncert/client.conf"
        parser.add_argument("-h","--help",action="store_true",dest="help",default=False,required=False)
        parser.add_argument("-c","--ssl_cert",action="store",dest="ssl_cert",required=True)
        parser.add_argument("-p","--ssl_prv",action="store",dest="ssl_prv",required=True)
        parser.add_argument("-i","--ca_index",action="store",dest="ca_index",default=0,required=False)
        parser.add_argument("-n","--id_name",action="store",dest="id_name",required=False)
        parser.add_argument("-v","--validity",action="store",dest="validity",default=100,required=False)
        parser.add_argument("-g","--config_file",action="store",dest="config_file",default=default_client_conf,required=False)

        # Interpret Informational Arguments
        interpret_help()

        # Getting all Arguments
        vars = parser.parse_args()

        # Process args
        args = {}
        args["ssl_cert"] = vars.ssl_cert
        args["ssl_prv"] = vars.ssl_prv
        args["ca_index"] = vars.ca_index
        args["id_name"] = vars.id_name
        args["validity"] = vars.validity
        args["config_file"] = vars.config_file

        # parse client config
        json_config = None
        ca_prefix = None
        default_id_name = None
        try:
            with open(args["config_file"], 'r', encoding='utf-8') as json_file:
                json_data = json_file.read()
                json_config = json.loads(json_data)
        except FileNotFoundError:
            raise FileNotFoundError(f'could not find config file: {args["config_file"]}') from None
        if json_config is not None:
            try:
                ca_prefix = json_config['ca-list'][args["ca_index"]]['ca-prefix']
            except IndexError:
                raise IndexError(f'could not find CA prefix at index: {args["ca_index"]}') from None

        print("loading SSL Certificate from PEM file")
        with open(args["ssl_cert"], "rb") as pem_file:
            pem_data = pem_file.read()
            cert = x509.load_pem_x509_certificate(pem_data, default_backend())
            attrbute_list = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            # obtain default name if not specified
            if attrbute_list is not None:
                common_name = attrbute_list[0].value
                if args["id_name"] is None:
                    args["id_name"] = ca_prefix + '/' + common_name
        return args

    args = parse_cmd_opts()
    return args

class NdncertDummyClient:
    def __init__(self, config: Dict):
        self.config = config

    def write_stdin(self, input):
        self.proc_handle.stdin.write(bytes(str(input) + '\n', "utf-8"))

    def run(self):
        self.proc_handle = subprocess.Popen(['ndncert-client', '-c', self.config["config_file"]], stdin=subprocess.PIPE)
        self.write_stdin(self.config["ca_index"])
        self.write_stdin("YES")
        self.write_stdin(self.config["id_name"])
        self.write_stdin(self.config["validity"])
        # selecting possesion challenge
        self.write_stdin("2")
        self.write_stdin("") # skip the other option
        self.write_stdin(self.config["ssl_cert"])
        self.write_stdin(self.config["ssl_prv"])

def main() -> int:
    default_config = {
        'ssl_cert': None,
        'ssl_prv': None,
        'ca_index': 0,
        'id_name': None,
        'validaty': 200,
        'config_file': None
    }
    cmd_args = process_cmd_opts()
    config = default_config.copy()
    config.update(cmd_args)
    NdncertDummyClient(config).run()
    return 0

if __name__ == "__main__":
    sys.exit(main())