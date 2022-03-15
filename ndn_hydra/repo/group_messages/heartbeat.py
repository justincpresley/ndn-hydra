# -------------------------------------------------------------
# NDN Hydra Heartbeat Group Message
# -------------------------------------------------------------
#  @Project: NDN Hydra
#  @Date:    2021-01-25
#  @Authors: Please check AUTHORS.rst
#  @Source-Code:   https://github.com/justincpresley/ndn-hydra
#  @Documentation: https://ndn-hydra.readthedocs.io
#  @Pip-Library:   https://pypi.org/project/ndn-hydra
# -------------------------------------------------------------

from typing import Callable
from ndn.encoding import *
import json
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage

class HeartbeatMessageTypes:
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

class HeartbeatMessageTlv(TlvModel):
    node_name = BytesField(HeartbeatMessageTypes.NODE_NAME)
    expire_at = UintField(HeartbeatMessageTypes.EXPIRE_AT)
    favor = BytesField(HeartbeatMessageTypes.FAVOR)

class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        val = "[MSG][HB] nam={nam};exp={exp};fav={fav}".format(
            nam=node_name,
            exp=expire_at,
            fav=favor
        )
        self.logger.debug(val)
        global_view.update_node(node_name, expire_at, favor, self.seqno)
        # sessions = global_view.get_sessions()
        # print(json.dumps(sessions))
        return
