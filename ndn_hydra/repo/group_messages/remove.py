# -------------------------------------------------------------
# NDN Hydra Remove Group Message
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
from ndn_hydra.repo.modules.global_view import GlobalView
from ndn_hydra.repo.group_messages.specific_message import SpecificMessage

class RemoveMessageTypes:
    NODE_NAME = 84
    EXPIRE_AT = 85
    FAVOR = 86

class RemoveMessageTlv(TlvModel):
    node_name = BytesField(RemoveMessageTypes.NODE_NAME)
    expire_at = UintField(RemoveMessageTypes.EXPIRE_AT)
    favor = BytesField(RemoveMessageTypes.FAVOR)
    file_name = NameField()

class RemoveMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(RemoveMessage, self).__init__(nid, seqno)
        self.message = RemoveMessageTlv.parse(raw_bytes)

    async def apply(self, global_view: GlobalView, fetch_file: Callable, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        expire_at = self.message.expire_at
        favor = float(self.message.favor.tobytes().decode())
        file_name = Name.to_str(self.message.file_name)
        val = "[MSG][REMOVE]  fil={fil}".format(
            fil=file_name
        )
        self.logger.info(val)
        # if insertion
        file = global_view.get_file(file_name)
        if (file == None) or (file['is_deleted'] == True):
            # add store to pending_stores
            self.logger.warning('nothing to remove')
        else:
            global_view.delete_file(file_name)
            # TODO: remove from data_storage
        # update session
        global_view.update_node(node_name, expire_at, favor, self.seqno)