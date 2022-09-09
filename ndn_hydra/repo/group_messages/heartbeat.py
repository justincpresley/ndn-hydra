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
from ndn_hydra.repo.modules.favor_calculator import FavorCalculator
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage

class HeartbeatMessageTypes:
    NODE_NAME = 84
    LATITUDE = 85
    LONGITUDE = 86
class HeartbeatMessageTlv(TlvModel):
    node_name = BytesField(HeartbeatMessageTypes.NODE_NAME)
    latitude = BytesField(HeartbeatMessageTypes.LATITUDE)
    longitude = BytesField(HeartbeatMessageTypes.LONGITUDE)

class HeartbeatMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(HeartbeatMessage, self).__init__(nid, seqno)
        self.message = HeartbeatMessageTlv.parse(raw_bytes)

    async def apply(self, global_view, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        favor = FavorCalculator().distance_based_favor(
            latA = float(config['latitude']), 
            lonA = float(config['longitude']),
            latB = float(self.message.latitude.tobytes().decode()),
            lonB = float(self.message.longitude.tobytes().decode()),
        )
        self.logger.debug(f"[MSG][HB]   nam={node_name};fav={favor}")
        global_view.update_node(node_name, favor, self.seqno)