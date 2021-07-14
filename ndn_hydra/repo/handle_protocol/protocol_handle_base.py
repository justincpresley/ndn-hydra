# ----------------------------------------------------------
# NDN Hydra Protocol Handle Base
# ----------------------------------------------------------
# @Project: NDN Hydra
# @Date:    2021-01-25
# @Author:  Zixuan Zhong
# @Author:  Justin C Presley
# @Author:  Daniel Achee
# @Source-Code: https://github.com/UCLA-IRL/ndn-hydra
# @Pip-Library: https://pypi.org/project/ndn-hydra/
# ----------------------------------------------------------

import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, NonStrictName, FormalName
from ndn.encoding.tlv_model import DecodeError
from typing import List
from ndn_python_repo import Storage
from ndn_hydra.repo.protocol.repo_commands import RepoCommand
from ndn_hydra.repo.utils.pubsub import PubSub

class ProtocolHandle(object):
    """
    Interface for protocol interest handles
    """
    def __init__(self, app: NDNApp, data_storage: Storage, pb: PubSub, config: dict):
        self.app = app
        self.data_storage = data_storage
        self.pb = pb
        self.config = config
        self.logger = logging.getLogger()

    async def listen(self, prefix: Name):
        raise NotImplementedError
