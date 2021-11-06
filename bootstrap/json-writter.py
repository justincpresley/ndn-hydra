# ----------------------------------------------------------
# NDN Hydra Json Writter
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Authors: Please check AUTHORS.rst
# @Source-Code:   https://github.com/UCLA-IRL/ndn-hydra
# @Documentation: https://ndn-hydra.readthedocs.io/
# @Pip-Library:   https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import sys
import argparse
import os
import json
from typing import Dict

ca_cert = ""
with open("bootstrap/hydra-anchor.cert", "r", encoding='utf-8') as ca_encoded:
    ca_cert = ca_encoded.read()

with open("bootstrap/ndncert-client.conf", "r+", encoding='utf-8') as client_conf:
    json_data = client_conf.read()
    json_config = json.loads(json_data)
    json_config['ca-list'][0]['certificate'] = ca_cert
    client_conf.seek(0)
    client_conf.write(json.dumps(json_config, indent=2))
    client_conf.truncate()