# -------------------------------------------------------------
# NDN Hydra Command Table and Command Blocks
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import random
import string
from datetime import datetime
from ndn_hydra.repo.protocol.status_code import StatusCode

class InsertCommandBlock:
    __slots__ = ('client_curi','client_prefix','status','file','fetch_path')
    def __init__(self, client_curi, client_prefix, status, file, fetch_path) -> None:
        self.client_curi, self.client_prefix, self.status, self.file, self.fetch_path = client_curi, client_prefix, status, file, fetch_path

class DeleteCommandBlock:
    __slots__ = ('client_curi','client_prefix','status','file_name')
    def __init__(self, client_curi, client_prefix, status, file_name) -> None:
        self.client_curi, self.client_prefix, self.status, self.file_name = client_curi, client_prefix, status, file_name

class CommandTable:
    def __init__(self):
        self.commands = {}
    def map_insert(client_curi, client_prefix, file, fetch_path):
        curi = self.generate_curi("insert")
        self.commands[curi] = InsertCommandBlock(client_curi, client_prefix, StatusCode.FETCHING, file, fetch_path)
    def map_delete(client_curi, client_prefix, file_name):
        curi = self.generate_curi("delete")
        self.commands[curi] = DeleteCommandBlock(client_curi, client_prefix, StatusCode.STAND_BY, file_name)
    def get_map(self, curi: str):
        try:
            return self.commands[curi]
        except KeyError:
            return None
    def update_map(self, curi: str, status: StatusCode):
        block = self.get_map(curi)
        if block:
            block.status = status
            self.commands[curi] = block
    def _generate_command_id(self, n: int) -> str:
        return ''.join(random.choice(string.ascii_letters+string.digits+string.punctuation) for i in range(n))
    def generate_curi(self, type: str) -> str:
        datetime = datetime.now().strftime("/date(%d:%m:%Y)/time(%H:%M:%S)/")
        ret = "/" + type + datetime + self._generate_command_id(8)
        while self.get_map(ret) != None:
            ret = "/" + type + datetime + self._generate_command_id(8)
        return ret