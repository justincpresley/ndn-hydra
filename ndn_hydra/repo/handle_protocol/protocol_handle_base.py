# -------------------------------------------------------------
# NDN Hydra Protocol Handle Base
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

import logging
from ndn.app import NDNApp
from ndn.encoding import Name
from ndn.storage import Storage
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
