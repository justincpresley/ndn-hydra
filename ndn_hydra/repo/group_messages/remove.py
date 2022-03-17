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
    FAVOR = 86
class RemoveMessageTlv(TlvModel):
    node_name = BytesField(RemoveMessageTypes.NODE_NAME)
    favor = BytesField(RemoveMessageTypes.FAVOR)
    file_name = NameField()

class RemoveMessage(SpecificMessage):
    def __init__(self, nid:str, seqno:int, raw_bytes:bytes):
        super(RemoveMessage, self).__init__(nid, seqno)
        self.message = RemoveMessageTlv.parse(raw_bytes)

    async def apply(self, global_view, fetch_file, svs, config):
        node_name = self.message.node_name.tobytes().decode()
        file_name = Name.to_str(self.message.file_name)

        self.logger.info(f"[MSG][REMOVE]   fil={file_name}")
        file = global_view.get_file(file_name)
        if (file == None) or (file['is_deleted'] == True):
            self.logger.warning('nothing to remove')
        else:
            global_view.delete_file(file_name)
            # TODO: remove from data_storage

        global_view.update_node(node_name, float(self.message.favor.tobytes().decode()), self.seqno)