# ----------------------------------------------------------
# NDN Hydra Message Body Base
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import logging

class MessageBodyBase:
    def __init__(self, nid:str, seq:int):
        self.nid = nid
        self.seq = seq
        self.logger = logging.getLogger()

    async def apply(self, global_view):
        raise NotImplementedError