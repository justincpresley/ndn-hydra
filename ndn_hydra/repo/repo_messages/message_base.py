# ----------------------------------------------------------
# NDN Hydra Message Body Base
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Authors: Please check AUTHORS.rst
# @Source-Code:   https://github.com/UCLA-IRL/ndn-hydra
# @Documentation: https://ndn-hydra.readthedocs.io/
# @Pip-Library:   https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import logging

class MessageBodyBase:
    def __init__(self, nid:str, seq:int):
        self.nid = nid
        self.seq = seq
        self.logger = logging.getLogger()

    async def apply(self, global_view):
        raise NotImplementedError