# -------------------------------------------------------------
# NDN Hydra Store Group Message
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
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage
from ndn_hydra.repo.modules.global_view import GlobalView

class StoreMessageTypes:
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

class StoreMessageTlv(TlvModel):
    node_name = BytesField(StoreMessageTypes.NODE_NAME)
    expire_at = UintField(StoreMessageTypes.EXPIRE_AT)
    favor = BytesField(StoreMessageTypes.FAVOR)
    file_name = NameField()

class StoreMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(StoreMessage, self).__init__(nid, seqno)
        self.message = StoreMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        file_name = Name.to_str(self.message.file_name)
        val = "[MSG][STORE]   nam={nam};fil={fil}".format(
            nam=node_name,
            fil=file_name
        )
        self.logger.info(val)
        # if insertion
        file = global_view.get_file(file_name)
        if (file == None) or (file['is_deleted'] == True):
            # add store to pending_stores
            self.logger.warning('add to pending store')
            global_view.add_pending_store(file_name, node_name)
        else:
            global_view.store_file(file_name, node_name)
        # update session
        global_view.update_node(node_name, expire_at, favor, self.seqno)